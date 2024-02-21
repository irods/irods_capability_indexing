#include "configuration.hpp"
#include "plugin_specific_configuration.hpp"
#include "utilities.hpp"

#include <irods/MD5Strategy.hpp>
#include <irods/irods_hasher_factory.hpp>
#include <irods/irods_log.hpp>
#include <irods/irods_re_plugin.hpp>
#include <irods/irods_re_ruleexistshelper.hpp>
#include <irods/rodsErrorTable.h>
#include <irods/rsModAVUMetadata.hpp>

#define IRODS_QUERY_ENABLE_SERVER_SIDE_API
#include <irods/irods_query.hpp>

#define IRODS_IO_TRANSPORT_ENABLE_SERVER_SIDE_API
#include <irods/dstream.hpp>
#include <irods/transport/default_transport.hpp>

#define IRODS_FILESYSTEM_ENABLE_SERVER_SIDE_API
#include <irods/filesystem.hpp>

#include <boost/algorithm/string.hpp>
#include <boost/any.hpp>
#include <boost/format.hpp>

#include <boost/archive/iterators/base64_from_binary.hpp>
#include <boost/archive/iterators/ostream_iterator.hpp>
#include <boost/archive/iterators/transform_width.hpp>

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/url.hpp>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <fmt/format.h>
#include <nlohmann/json.hpp>

#include <algorithm>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

namespace
{
	namespace beast = boost::beast;
	namespace http = beast::http;

	using http_response = std::optional<http::response<http::string_body>>;
	using json = nlohmann::json;
	using string_t = std::string;

	struct configuration : irods::indexing::configuration
	{
		std::vector<std::string> hosts;
		int bulk_count{10};
		int read_size{4194304};
		std::string es_version{"7."}; // TODO Not used.

		configuration(const std::string& _instance_name)
			: irods::indexing::configuration(_instance_name)
		{
			try {
				auto cfg = irods::indexing::get_plugin_specific_configuration(_instance_name);

				if (auto iter = cfg.find("hosts"); iter != cfg.end()) {
					for (auto& i : *iter) {
						hosts.push_back(i.get<std::string>());
					}
				}
				else {
					THROW(USER_INPUT_OPTION_ERR, fmt::format("{}: elasticsearch: [hosts] cannot be empty", __func__));
				}

				if (auto iter = cfg.find("es_version"); iter != cfg.end()) {
					es_version = iter->get<std::string>();
				}

				if (auto iter = cfg.find("bulk_count"); iter != cfg.end()) {
					bulk_count = iter->get<int>();
					if (bulk_count <= 0) {
						THROW(USER_INPUT_OPTION_ERR,
						      fmt::format(
								  "{}: elasticsearch: Invalid value [{}] for [bulk_count]", __func__, bulk_count));
					}
				}

				if (auto iter = cfg.find("read_size"); iter != cfg.end()) {
					read_size = iter->get<int>();
					if (read_size <= 0) {
						THROW(
							USER_INPUT_OPTION_ERR,
							fmt::format("{}: elasticsearch: Invalid value [{}] for [read_size]", __func__, read_size));
					}
				}
			}
			catch (const std::exception& _e) {
				THROW(USER_INPUT_OPTION_ERR, _e.what());
			}
		}
	}; // struct configuration

	std::unique_ptr<configuration> config;
	std::string object_index_policy;
	std::string object_purge_policy;
	std::string metadata_index_policy;
	std::string metadata_purge_policy;

	auto send_http_request(http::verb _verb, const std::string_view _target, const std::string_view _body = "")
		-> http_response
	{
		for (auto&& host : config->hosts) {
			if (host.empty()) {
				rodsLog(LOG_ERROR, "%s: empty service URL.", __func__);
				continue;
			}

			namespace urls = boost::urls;

			urls::result<urls::url_view> result = urls::parse_uri(host);
			if (!result) {
				rodsLog(LOG_ERROR, fmt::format("{}: could not parse service URL [{}].", __func__, host).c_str());
				continue;
			}

			const auto use_tls = (result->has_scheme() && result->scheme_id() == urls::scheme::https);

			namespace net = boost::asio;
			using tcp = net::ip::tcp;

			// This lambda encapsulates the HTTP logic that's independent of the type of stream.
			const auto construct_and_send_http_request = [_verb, _target, _body](auto& _stream) {
				http::request<http::string_body> req{_verb, _target, 11};
				req.set(http::field::host, boost::asio::ip::host_name());
				req.set(http::field::user_agent, "iRODS Indexing Plugin/" IRODS_PLUGIN_VERSION);
				req.set(http::field::content_type, "application/json");

				if (!_body.empty()) {
					req.body() = _body;

					std::stringstream ss;
					ss << req;
					const auto s = ss.str();

					if (s.size() > 256) {
						rodsLog(LOG_DEBUG,
						        fmt::format("{}: sending request = (truncated) [{} ...]", __func__, s.substr(0, 256))
						            .c_str());
					}
					else {
						rodsLog(LOG_DEBUG, fmt::format("{}: sending request = [{}]", __func__, s).c_str());
					}

					req.prepare_payload();
				}

				http::write(_stream, req);

				beast::flat_buffer buffer;
				http::response<http::string_body> res;
				http::read(_stream, buffer, res);

				std::stringstream ss;
				ss << res;
				rodsLog(LOG_DEBUG, fmt::format("{}: elasticsearch response = [{}]", __func__, ss.str()).c_str());

				return res;
			};

			try {
				net::io_context ioc;

				tcp::resolver resolver{ioc};
				const auto results = resolver.resolve(result->host(), result->port());

				if (use_tls) {
					net::ssl::context tls_ctx{net::ssl::context::tlsv12_client};
					tls_ctx.set_default_verify_paths();
					tls_ctx.set_verify_mode(net::ssl::verify_peer);

					beast::ssl_stream<beast::tcp_stream> stream{ioc, tls_ctx};

					// Set SNI hostname (many hosts need this to handshake successfully).
					if (!::SSL_set_tlsext_host_name(stream.native_handle(), result->host().c_str())) {
						beast::error_code ec{static_cast<int>(::ERR_get_error()), net::error::get_ssl_category()};
						throw beast::system_error{ec};
					}

					beast::get_lowest_layer(stream).connect(results);

					auto res = construct_and_send_http_request(stream);

					beast::error_code ec;
					stream.shutdown(ec);

					if (net::error::eof == ec) {
						// Rationale:
						// http://stackoverflow.com/questions/25587403/boost-asio-ssl-async-shutdown-always-finishes-with-an-error
						ec = {};
					}

					if (ec) {
						throw beast::system_error{ec};
					}

					return res;
				}
				else {
					beast::tcp_stream stream{ioc};
					stream.connect(results);

					auto res = construct_and_send_http_request(stream);

					beast::error_code ec;
					stream.socket().shutdown(tcp::socket::shutdown_both, ec);

					// not_connected happens sometimes, so don't bother reporting it.
					if (ec && ec != beast::errc::not_connected) {
						throw beast::system_error{ec};
					}

					return res;
				}
			}
			catch (const std::exception& e) {
				rodsLog(LOG_ERROR, fmt::format("{}: {}", __func__, e.what()).c_str());
			}
		}

		return std::nullopt;
	} // send_http_request

	std::string generate_id()
	{
		using namespace boost::archive::iterators;
		std::stringstream os;
		typedef base64_from_binary< // convert binary values to base64 characters
			transform_width<        // retrieve 6 bit integers from a sequence of 8 bit bytes
				const char*,
				6,
				8>>
			base64_text; // compose all the above operations in to a new iterator

		boost::uuids::uuid uuid{boost::uuids::random_generator()()};
		std::string uuid_str = boost::uuids::to_string(uuid);
		std::copy(
			base64_text(uuid_str.c_str()), base64_text(uuid_str.c_str() + uuid_str.size()), ostream_iterator<char>(os));

		return os.str();
	} // generate_id

	std::string get_object_index_id(ruleExecInfo_t* _rei, const std::string& _object_path, bool* iscoll = nullptr)
	{
		boost::filesystem::path p{_object_path};
		std::string coll_name = p.parent_path().string();
		std::string data_name = p.filename().string();
		namespace fs = irods::experimental::filesystem;
		namespace fsvr = irods::experimental::filesystem::server;
		std::string query_str;
		if (fsvr::is_collection(*_rei->rsComm, fs::path{_object_path})) {
			if (iscoll) {
				*iscoll = true;
			}
			query_str = fmt::format("SELECT COLL_ID WHERE COLL_NAME = '{}'", _object_path);
		}
		else {
			if (iscoll) {
				*iscoll = false;
			}
			query_str = fmt::format("SELECT DATA_ID WHERE COLL_NAME = '{}' AND DATA_NAME = '{}'", coll_name, data_name);
		}
		try {
			irods::query<rsComm_t> qobj{_rei->rsComm, query_str, 1};
			if (qobj.size() > 0) {
				return qobj.front()[0];
			}
			THROW(CAT_NO_ROWS_FOUND, boost::format("failed to get object id for [%s]") % _object_path);
		}
		catch (const irods::exception& _e) {
			THROW(CAT_NO_ROWS_FOUND, boost::format("failed to get object id for [%s]") % _object_path);
		}

	} // get_object_index_id

	void get_metadata_for_object_index_id(ruleExecInfo_t* _rei,
	                                      std::string _obj_id,
	                                      bool _is_coll,
	                                      std::optional<nlohmann::json>& _out)
	{
		if (!_out || !_out->is_array()) {
			_out = nlohmann::json::array();
		}

		auto& avus_out = *_out;
		const std::string query_str =
			_is_coll ? fmt::format("SELECT META_COLL_ATTR_NAME, META_COLL_ATTR_VALUE, META_COLL_ATTR_UNITS"
		                           " WHERE COLL_ID = '{}' ",
		                           _obj_id)
					 : fmt::format("SELECT META_DATA_ATTR_NAME, META_DATA_ATTR_VALUE, META_DATA_ATTR_UNITS"
		                           " WHERE DATA_ID = '{}' ",
		                           _obj_id);
		irods::query<rsComm_t> qobj{_rei->rsComm, query_str};
		for (const auto& row : qobj) {
			if (row[0] == config->index)
				continue;
			avus_out += {{"attribute", row[0]}, {"value", row[1]}, {"unit", row[2]}};
		}
	} // get_metadata_for_object_index_id

	void update_object_metadata(ruleExecInfo_t* _rei,
	                            const std::string& _object_path,
	                            const std::string& _attribute,
	                            const std::string& _value,
	                            const std::string& _units)
	{
		modAVUMetadataInp_t set_op{.arg0 = "set",
		                           .arg1 = "-d",
		                           .arg2 = const_cast<char*>(_object_path.c_str()),
		                           .arg3 = const_cast<char*>(_attribute.c_str()),
		                           .arg4 = const_cast<char*>(_value.c_str()),
		                           .arg5 = const_cast<char*>(_units.c_str())};

		auto status = rsModAVUMetadata(_rei->rsComm, &set_op);
		if (status < 0) {
			THROW(status, boost::format("failed to update object [%s] metadata") % _object_path);
		}
	} // update_object_metadata

	void invoke_indexing_event_full_text(ruleExecInfo_t* _rei,
	                                     const std::string& _object_path,
	                                     const std::string& _source_resource,
	                                     const std::string& _index_name)
	{
		try {
			const std::string object_id = get_object_index_id(_rei, _object_path);
			std::vector<char> buffer(config->read_size);
			irods::experimental::io::server::basic_transport<char> xport(*_rei->rsComm);
			irods::experimental::io::idstream in{xport, _object_path};

			int chunk_counter{0};
			bool need_final_perform{false};
			std::stringstream ss;

			while (in) {
				in.read(buffer.data(), buffer.size());

				// The indexing instruction.
				// clang-format off
				ss << json{{"index", {
					{"_id", fmt::format("{}_{}", object_id, chunk_counter++)}
				}}}.dump() << '\n';
				// clang-format on

				// The defaults for the .dump() member function.
				constexpr int indent = -1;
				constexpr char indent_char = ' ';
				constexpr bool ensure_ascii = false;

				// The data to index.
				// The version of .dump() invoked here instructs the library to ignore
				// invalid UTF-8 sequences. All bytes are copied to the output unchanged.
				// clang-format off
				ss << json{
					{"absolutePath", _object_path},
					{"data", std::string_view(buffer.data(), in.gcount())}
				}.dump(indent, indent_char, ensure_ascii, json::error_handler_t::ignore) << '\n';
				// clang-format on

				// Send bulk request if chunk counter has reached bulk limit.
				if (chunk_counter == config->bulk_count) {
					chunk_counter = 0;

					const auto res = send_http_request(http::verb::post, _index_name + "/_bulk", ss.str());

					if (!res.has_value()) {
						rodsLog(LOG_ERROR, "%s: No response from elasticsearch host.", __func__);
					}
					else {
						rodsLog(LOG_ERROR,
						        "%s: Error sending request to elasticsearch host. [http_status_code=[%d]]",
						        __func__,
						        res->result_int());
					}

					ss.str("");
				}
			}

			if (chunk_counter > 0) {
				// Elasticsearch limits the maximum size of a HTTP request to 100mb.
				// See https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html.
				const auto res = send_http_request(http::verb::post, _index_name + "/_bulk", ss.str());

				if (!res.has_value()) {
					rodsLog(LOG_ERROR, "%s: No response from elasticsearch host.", __func__);
				}
				else {
					rodsLog(LOG_ERROR,
					        "%s: Error sending request to elasticsearch host. [http_status_code=[%d]]",
					        __func__,
					        res->result_int());
				}
			}
		}
		catch (const irods::exception& _e) {
			rodsLog(LOG_ERROR, "Exception [%s]", _e.what());
			auto irods_error = _e.code();
			if (irods_error != CAT_NO_ROWS_FOUND) {
				THROW(irods_error, _e.what());
			}
		}
		catch (const std::runtime_error& _e) {
			rodsLog(LOG_ERROR, "Exception [%s]", _e.what());
			THROW(SYS_INTERNAL_ERR, _e.what());
		}
		catch (const std::exception& _e) {
			rodsLog(LOG_ERROR, "Exception [%s]", _e.what());
			THROW(SYS_INTERNAL_ERR, _e.what());
		}
	} // invoke_indexing_event_full_text

	void invoke_purge_event_full_text(ruleExecInfo_t* _rei,
	                                  const std::string& _object_path,
	                                  const std::string& _source_resource,
	                                  const std::string& _index_name)
	{
		try {
			const std::string object_id{get_object_index_id(_rei, _object_path)};
			int chunk_counter{0};

			while (true) {
				const auto index_entry = fmt::format("{}/_doc/{}_{}", _index_name, object_id, chunk_counter++);
				const auto response = send_http_request(http::verb::delete_, index_entry);

				if (!response.has_value()) {
					rodsLog(LOG_ERROR,
					        "%s: No response from elasticsearch host. Index entry [%s] may not have been purged",
					        __func__,
					        index_entry.c_str());
					break;
				}

				// Some objects will be split into multiple chunks. Because we don't track them,
				// the only way to know when all chunks have been processed is to send requests until
				// we receive a HTTP status code of 404. Here, we expand the status code range to
				// anything other than 200.
				if (response->result_int() != 200) {
					if (response->result_int() == 404) { // meaningful for logging
						rodsLog(LOG_NOTICE,
						        fmt::format("{}: No index entry for chunk [{}] of object_id [{}] in index [{}]",
						                    __func__,
						                    chunk_counter,
						                    object_id,
						                    _index_name)
						            .c_str());
					}

					break;
				}
			}
		}
		catch (const std::runtime_error& _e) {
			rodsLog(LOG_ERROR, "Exception [%s]", _e.what());
			THROW(SYS_INTERNAL_ERR, _e.what());
		}
		catch (const irods::exception& _e) {
			if (_e.code() == CAT_NO_ROWS_FOUND) {
				return;
			}
			THROW(SYS_INTERNAL_ERR, _e.what());
		}
		catch (const std::exception& _e) {
			rodsLog(LOG_ERROR, "Exception [%s]", _e.what());
			THROW(SYS_INTERNAL_ERR, _e.what());
		}
	} // invoke_purge_event_full_text

	std::string get_metadata_index_id(const std::string& _index_id,
	                                  const std::string& _attribute,
	                                  const std::string& _value,
	                                  const std::string& _units)
	{
		std::string str = _attribute + _value + _units;
		irods::Hasher hasher;
		irods::getHasher(irods::MD5_NAME, hasher);
		hasher.update(str);

		std::string digest;
		hasher.digest(digest);

		return _index_id + irods::indexing::indexer_separator + digest;

	} // get_metadata_index_id

	void invoke_indexing_event_metadata(ruleExecInfo_t* _rei,
	                                    const std::string& _object_path,
	                                    const std::string& _attribute,
	                                    const std::string& _value,
	                                    const std::string& _unit,
	                                    const std::string& _index_name,
	                                    nlohmann::json& obj_meta)
	{
		try {
			bool is_coll{};
			auto object_id = get_object_index_id(_rei, _object_path, &is_coll);

			std::optional<nlohmann::json> jsonarray;
			get_metadata_for_object_index_id(_rei, object_id, is_coll, jsonarray);
			if (!jsonarray) {
				irods::log(LOG_WARNING,
				           fmt::format(
							   "In {}, function {}: Aborted indexing metadata, null AVU array returned for object [{}]",
							   __FILE__,
							   __func__,
							   _object_path));
				return;
			}
			obj_meta["metadataEntries"] = *jsonarray;

			const auto response =
				send_http_request(http::verb::put, fmt::format("{}/_doc/{}", _index_name, object_id), obj_meta.dump());

			if (!response.has_value()) {
				THROW(SYS_INTERNAL_ERR,
				      fmt::format("failed to index metadata [{}] [{}] [{}] for [{}]. No response.",
				                  _attribute,
				                  _value,
				                  _unit,
				                  _object_path));
			}

			if (response->result_int() != 200 && response->result_int() != 201) {
				THROW(SYS_INTERNAL_ERR,
				      fmt::format("failed to index metadata [{}] [{}] [{}] for [{}] code [{}] message [{}]",
				                  _attribute,
				                  _value,
				                  _unit,
				                  _object_path,
				                  response->result_int(),
				                  response->body()));
			}
		}
		catch (const irods::exception& _e) {
			rodsLog(LOG_ERROR, "Exception [%s]", _e.what());
			auto irods_error = _e.code();
			if (irods_error != CAT_NO_ROWS_FOUND) {
				THROW(irods_error, _e.what());
			}
		}
		catch (const std::runtime_error& _e) {
			rodsLog(LOG_ERROR, "Exception [%s]", _e.what());
			THROW(SYS_INTERNAL_ERR, _e.what());
		}
		catch (const std::exception& _e) {
			rodsLog(LOG_ERROR, "Exception [%s]", _e.what());
			THROW(SYS_INTERNAL_ERR, _e.what());
		}
	} // invoke_indexing_event_metadata

	void invoke_purge_event_metadata(ruleExecInfo_t* _rei,
	                                 const std::string& _object_path,
	                                 const std::string& _attribute,
	                                 const std::string& _value,
	                                 const std::string& _unit,
	                                 const std::string& _index_name,
	                                 const nlohmann::json& = {})
	{
		try {
			namespace fs = irods::experimental::filesystem;

			// we now accept object id or path here, so pep_api_rm_coll_post can purge
			const auto object_id =
				fs::path{_object_path}.is_absolute() ? get_object_index_id(_rei, _object_path) : _object_path;

			const auto response =
				send_http_request(http::verb::delete_, fmt::format("{}/_doc/{}", _index_name, object_id));

			if (!response.has_value()) {
				auto msg = fmt::format("{}: No response from elasticsearch host.", __func__);
				rodsLog(LOG_ERROR, msg.c_str());
				THROW(SYS_INTERNAL_ERR, std::move(msg));
			}

			switch (response->result_int()) {
				// either the index has been deleted, or the AVU was cleared unexpectedly
				case 404:
					rodsLog(LOG_NOTICE,
					        fmt::format("received HTTP status code of 404: no index entry for AVU ({}, {}, {}) on "
					                    "object [{}] in index [{}]",
					                    _attribute,
					                    _value,
					                    _unit,
					                    _object_path,
					                    _index_name)
					            .c_str());
					break;
				// routinely expected return codes ( not logged ):
				case 200:
				case 201:
					break;
				// unexpected return codes:
				default:
					THROW(SYS_INTERNAL_ERR,
					      fmt::format("failed to index metadata [{}] [{}] [{}] for [{}] code [{}] message [{}]",
					                  _attribute,
					                  _value,
					                  _unit,
					                  _object_path,
					                  response->result_int(),
					                  response->body()));
			}
		}
		catch (const std::runtime_error& _e) {
			rodsLog(LOG_ERROR, "Exception [%s]", _e.what());
			THROW(SYS_INTERNAL_ERR, _e.what());
		}
		catch (const std::exception& _e) {
			rodsLog(LOG_ERROR, "Exception [%s]", _e.what());
			THROW(SYS_INTERNAL_ERR, _e.what());
		}
	} // invoke_purge_event_metadata

	irods::error start(irods::default_re_ctx&, const std::string& _instance_name)
	{
		RuleExistsHelper::Instance()->registerRuleRegex("irods_policy_.*");

		try {
			config = std::make_unique<configuration>(_instance_name);
		}
		catch (const irods::exception& e) {
			return e;
		}

		object_index_policy =
			irods::indexing::policy::compose_policy_name(irods::indexing::policy::object::index, "elasticsearch");
		object_purge_policy =
			irods::indexing::policy::compose_policy_name(irods::indexing::policy::object::purge, "elasticsearch");
		metadata_index_policy =
			irods::indexing::policy::compose_policy_name(irods::indexing::policy::metadata::index, "elasticsearch");
		metadata_purge_policy =
			irods::indexing::policy::compose_policy_name(irods::indexing::policy::metadata::purge, "elasticsearch");

		return SUCCESS();
	}

	irods::error stop(irods::default_re_ctx&, const std::string&)
	{
		return SUCCESS();
	}

	irods::error rule_exists(irods::default_re_ctx&, const std::string& _rn, bool& _ret)
	{
		_ret = "irods_policy_recursive_rm_object_by_path" == _rn || object_index_policy == _rn ||
		       object_purge_policy == _rn || metadata_index_policy == _rn || metadata_purge_policy == _rn;
		return SUCCESS();
	}

	irods::error list_rules(irods::default_re_ctx&, std::vector<std::string>& _rules)
	{
		_rules.push_back(object_index_policy);
		_rules.push_back(object_purge_policy);
		_rules.push_back(metadata_index_policy);
		_rules.push_back(metadata_purge_policy);
		return SUCCESS();
	}

	irods::error exec_rule(irods::default_re_ctx&,
	                       const std::string& _rn,
	                       std::list<boost::any>& _args,
	                       irods::callback _eff_hdlr)
	{
		ruleExecInfo_t* rei{};
		const auto err = _eff_hdlr("unsafe_ms_ctx", &rei);

		if (!err.ok()) {
			return err;
		}

		using nlohmann::json;
		try {
			if (_rn == object_index_policy) {
				auto it = _args.begin();
				const std::string object_path{boost::any_cast<std::string>(*it)};
				++it;
				const std::string source_resource{boost::any_cast<std::string>(*it)};
				++it;
				const std::string index_name{boost::any_cast<std::string>(*it)};
				++it;

				invoke_indexing_event_full_text(rei, object_path, source_resource, index_name);
			}
			else if (_rn == object_purge_policy) {
				auto it = _args.begin();
				const std::string object_path{boost::any_cast<std::string>(*it)};
				++it;
				const std::string source_resource{boost::any_cast<std::string>(*it)};
				++it;
				const std::string index_name{boost::any_cast<std::string>(*it)};
				++it;

				invoke_purge_event_full_text(rei, object_path, source_resource, index_name);
			}
			else if (_rn == metadata_index_policy || _rn == metadata_purge_policy) {
				auto it = _args.begin();
				const std::string object_path{boost::any_cast<std::string>(*it)};
				++it;
				const std::string attribute{boost::any_cast<std::string>(*it)};
				++it;
				const std::string value{boost::any_cast<std::string>(*it)};
				++it;
				const std::string unit{boost::any_cast<std::string>(*it)};
				++it;
				const std::string index_name{boost::any_cast<std::string>(*it)};
				++it;

				std::string obj_meta_str = "{}";

				if (it != _args.end()) {
					obj_meta_str = boost::any_cast<std::string>(*it++);
				}

				json obj_meta = nlohmann::json::parse(obj_meta_str);

				// purge with AVU by name?
				if (_rn == metadata_purge_policy && attribute.empty()) {
					// delete the indexed entry
					invoke_purge_event_metadata(rei, object_path, attribute, value, unit, index_name);
				}
				else {
					// update the indexed entry
					invoke_indexing_event_metadata(rei, object_path, attribute, value, unit, index_name, obj_meta);
				}
			}
			else if (_rn == "irods_policy_recursive_rm_object_by_path") {
				auto it = _args.begin();
				const std::string the_path{boost::any_cast<std::string>(*it)};
				std::advance(it, 2);
				const json recurse_info = json::parse(boost::any_cast<std::string&>(*it));

				const auto escaped_path = [p = the_path]() mutable {
					boost::replace_all(p, "\\", "\\\\");
					boost::replace_all(p, "?", "\\?");
					boost::replace_all(p, "*", "\\*");
					return p;
				}();

				std::string JtopLevel = json{{"query", {{"match", {{"absolutePath", escaped_path}}}}}}.dump();
				std::string JsubObject;

				try {
					if (recurse_info.at("is_collection").get<bool>()) {
						JsubObject =
							json{{"query", {{"wildcard", {{"absolutePath", {{"value", escaped_path + "/*"}}}}}}}}
								.dump();
					}
				}
				catch (const json::exception& e) {
					return ERROR(SYS_LIBRARY_ERROR, e.what());
				}

				try {
					for (const auto& e : recurse_info.at("indices")) {
						const auto& index_name = e.get_ref<const std::string&>();

						for (const std::string& query_input : {JtopLevel, JsubObject}) {
							if (query_input.empty()) {
								continue;
							}

							const auto response = send_http_request(
								http::verb::post, fmt::format("{}/_delete_by_query", index_name), query_input);

							if (!response.has_value()) {
								rodsLog(LOG_ERROR,
								        fmt::format("{}: No response from elasticsearch host.", __func__).c_str());
								continue;
							}

							if (response->result_int() != 200) {
								rodsLog(
									LOG_WARNING,
									fmt::format(
										"{}: _delete_by_query failed [rule=[{}], path=[{}]", __func__, _rn, the_path)
										.c_str());
							}
						}
					}
				}
				catch (const nlohmann::json::parse_error& e) {
					rodsLog(LOG_ERROR, fmt::format("JSON parse exception : [{}]", e.what()).c_str());
				}
			} // "irods_policy_recursive_rm_object_by_path"
			else {
				return ERROR(SYS_NOT_SUPPORTED, _rn);
			}
		}
		catch (const std::invalid_argument& _e) {
			irods::indexing::exception_to_rerror(SYS_NOT_SUPPORTED, _e.what(), rei->rsComm->rError);
			return ERROR(SYS_NOT_SUPPORTED, _e.what());
		}
		catch (const boost::bad_any_cast& _e) {
			irods::indexing::exception_to_rerror(INVALID_ANY_CAST, _e.what(), rei->rsComm->rError);
			return ERROR(SYS_NOT_SUPPORTED, _e.what());
		}
		catch (const irods::exception& _e) {
			irods::indexing::exception_to_rerror(_e, rei->rsComm->rError);
			return irods::error(_e);
		}

		return err;
	} // exec_rule

	irods::error exec_rule_text(irods::default_re_ctx&,
	                            const std::string&,
	                            msParamArray_t*,
	                            const std::string&,
	                            irods::callback)
	{
		return ERROR(RULE_ENGINE_CONTINUE, "exec_rule_text is not supported");
	} // exec_rule_text

	irods::error exec_rule_expression(irods::default_re_ctx&, const std::string&, msParamArray_t*, irods::callback)
	{
		return ERROR(RULE_ENGINE_CONTINUE, "exec_rule_expression is not supported");
	} // exec_rule_expression
} // namespace

extern "C" irods::pluggable_rule_engine<irods::default_re_ctx>* plugin_factory(const std::string& _inst_name,
                                                                               const std::string& _context)
{
	irods::pluggable_rule_engine<irods::default_re_ctx>* re =
		new irods::pluggable_rule_engine<irods::default_re_ctx>(_inst_name, _context);
	re->add_operation<irods::default_re_ctx&, const std::string&>(
		"start", std::function<irods::error(irods::default_re_ctx&, const std::string&)>(start));
	re->add_operation<irods::default_re_ctx&, const std::string&>(
		"stop", std::function<irods::error(irods::default_re_ctx&, const std::string&)>(stop));
	re->add_operation<irods::default_re_ctx&, const std::string&, bool&>(
		"rule_exists", std::function<irods::error(irods::default_re_ctx&, const std::string&, bool&)>(rule_exists));
	re->add_operation<irods::default_re_ctx&, std::vector<std::string>&>(
		"list_rules", std::function<irods::error(irods::default_re_ctx&, std::vector<std::string>&)>(list_rules));
	re->add_operation<irods::default_re_ctx&, const std::string&, std::list<boost::any>&, irods::callback>(
		"exec_rule",
		std::function<irods::error(
			irods::default_re_ctx&, const std::string&, std::list<boost::any>&, irods::callback)>(exec_rule));
	re->add_operation<irods::default_re_ctx&, const std::string&, msParamArray_t*, const std::string&, irods::callback>(
		"exec_rule_text",
		std::function<irods::error(
			irods::default_re_ctx&, const std::string&, msParamArray_t*, const std::string&, irods::callback)>(
			exec_rule_text));

	re->add_operation<irods::default_re_ctx&, const std::string&, msParamArray_t*, irods::callback>(
		"exec_rule_expression",
		std::function<irods::error(irods::default_re_ctx&, const std::string&, msParamArray_t*, irods::callback)>(
			exec_rule_expression));
	return re;
} // plugin_factory
