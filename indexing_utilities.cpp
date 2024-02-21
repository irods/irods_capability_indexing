#include "indexing_utilities.hpp"

#include "cpp_json_kw.hpp"
#include "utilities.hpp"

#include <irods/irods_at_scope_exit.hpp>
#include <irods/irods_re_plugin.hpp>
#include <irods/irods_server_properties.hpp>
#include <irods/irods_virtual_path.hpp>
#include <irods/rsCloseCollection.hpp>
#include <irods/rsExecMyRule.hpp>
#include <irods/rsModAVUMetadata.hpp>
#include <irods/rsOpenCollection.hpp>
#include <irods/rsReadCollection.hpp>

#ifndef IRODS_METADATA_ENABLE_SERVER_SIDE_API
#  define IRODS_METADATA_ENABLE_SERVER_SIDE_API
#endif
#include <irods/metadata.hpp>

#ifndef IRODS_QUERY_ENABLE_SERVER_SIDE_API
#  define IRODS_QUERY_ENABLE_SERVER_SIDE_API
#endif
#include <irods/irods_query.hpp>

#ifndef IRODS_FILESYSTEM_ENABLE_SERVER_SIDE_API
#  define IRODS_FILESYSTEM_ENABLE_SERVER_SIDE_API
#endif
#include <irods/filesystem.hpp>

#include <boost/algorithm/string.hpp>
#include <boost/any.hpp>
#include <boost/exception/all.hpp>
#include <boost/format.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/regex.hpp>

#include <fmt/format.h>
#include <nlohmann/json.hpp>

#include <unistd.h>

#include <functional>
#include <limits>
#include <random>

using namespace std::string_literals;

int _delayExec(const char* inActionCall,
               const char* recoveryActionCall,
               const char* delayCondition,
               ruleExecInfo_t* rei);

namespace irods::indexing
{
	indexer::indexer(ruleExecInfo_t* _rei, const std::string& _instance_name)
		: rei_(_rei)
		, comm_(_rei->rsComm)
		, config_(_instance_name)
	{
	} // indexer

	// -=-=-=-=  Launch a new delayed task to execute index or purge operation
	// -
	void indexer::schedule_indexing_policy(const std::string& _json, const std::string& _params)
	{
		const int delay_err = _delayExec(_json.c_str(), "", _params.c_str(), rei_);
		if (delay_err < 0) {
			THROW(delay_err, "delayExec failed");
		}
	} // schedule_indexing_policy

	// -=-=-=-= Does the given AVU exist on the collection of the given name?
	// -
	bool indexer::metadata_exists_on_collection(const std::string& _collection_name,
	                                            const std::string& _attribute,
	                                            const std::string& _value,
	                                            const std::string& _units)
	{
		try {
			std::string query_str{boost::str(
				boost::format("SELECT META_COLL_ATTR_VALUE, META_COLL_ATTR_UNITS WHERE META_COLL_ATTR_NAME = '%s' "
			                  "and COLL_NAME = '%s'") %
				_attribute % _collection_name)};
			query<rsComm_t> qobj{rei_->rsComm, query_str, 1};

			if (qobj.size() == 0) {
				return false;
			}

			for (auto results : qobj) {
				if (results[0] == _value && results[1] == _units) {
					return true;
				}
			}

			return false;
		}
		catch (irods::exception& _e) {
			return false;
		}
	} // metadata_exists_on_collection

	void indexer::schedule_collection_operation(const std::string& _operation_type,
	                                            const std::string& _collection_name,
	                                            const std::string& _user_name,
	                                            const std::string& _indexer_string,
	                                            const std::string& _indexer)
	{
		rodsLog(config_.log_level,
		        "irods::indexing::collection indexing collection [%s] with [%s] type [%s]",
		        _collection_name.c_str(),
		        _indexer_string.c_str(),
		        _indexer.c_str());

		std::string index_name, index_type;
		std::tie(index_name, index_type) = parse_indexer_string(_indexer_string);
		const auto policy_name = _operation_type == irods::indexing::operation_type::index
		                             ? irods::indexing::policy::collection::index
		                             : irods::indexing::policy::collection::purge;
		using json = nlohmann::json;
		json rule_obj;
		rule_obj["rule-engine-operation"] = policy_name;
		rule_obj["rule-engine-instance-name"] = config_.instance_name;
		rule_obj["collection-name"] = _collection_name;
		rule_obj["user-name"] = _user_name;
		rule_obj["indexer"] = _indexer;
		rule_obj["index-name"] = index_name;
		rule_obj["index-type"] = index_type;

		const auto delay_err =
			_delayExec(rule_obj.dump().c_str(), "", generate_delay_execution_parameters().c_str(), rei_);
		if (delay_err < 0) {
			THROW(delay_err,
			      boost::format("queue collection indexing failed for [%s] indexer [%s] type [%s]") % _collection_name %
			          _indexer % index_type);
		}

		rodsLog(config_.log_level,
		        "irods::indexing::collection indexing collection [%s] with [%s] type [%s]",
		        _collection_name.c_str(),
		        _indexer.c_str(),
		        index_type.c_str());
	} // schedule_collection_operation

	std::vector<std::string> indexer::get_indexing_resource_names()
	{
		std::string query_str{boost::str(
			boost::format("SELECT RESC_NAME WHERE META_RESC_ATTR_NAME = '%s' AND META_RESC_ATTR_VALUE = 'true'") %
			config_.index)};

		query<rsComm_t> qobj{comm_, query_str};
		std::vector<std::string> ret_val;
		for (const auto& row : qobj) {
			ret_val.push_back(row[0]);
		}

		return ret_val;

	} // get_indexing_resource_names

	std::string indexer::get_indexing_resource_name_for_object(const std::string _object_path,
	                                                           std::vector<std::string> _resource_names)
	{
		boost::filesystem::path p{_object_path};
		std::string coll_name = p.parent_path().string();
		std::string data_name = p.filename().string();

		std::string query_str{boost::str(boost::format("SELECT RESC_NAME WHERE DATA_NAME = '%s' AND COLL_NAME = '%s'") %
		                                 data_name % coll_name)};
		query<rsComm_t> qobj{comm_, query_str, 1};
		if (qobj.size() == 0) {
			THROW(CAT_NO_ROWS_FOUND, boost::format("no resource names found for object [%s]") % _object_path);
		}

		if (_resource_names.empty()) {
			return qobj.front()[0];
		}

		for (const auto& resource_name_for_object : qobj) {
			for (const auto& resource_name_for_indexing : _resource_names) {
				if (resource_name_for_object[0] == resource_name_for_indexing) {
					return resource_name_for_object[0];
				}
			}
		}

		THROW(
			SYS_INVALID_INPUT_PARAM, boost::format("failed to find indexing resource for object [%s]") % _object_path);

	} // get_indexing_resource_name_for_object

	bool indexer::resource_is_indexable(const std::string _source_resource, std::vector<std::string> _resource_names)
	{
		if (_source_resource == EMPTY_RESOURCE_NAME || _resource_names.empty()) {
			return true;
		}

		for (const auto& resource_name_for_indexing : _resource_names) {
			if (_source_resource == resource_name_for_indexing) {
				return true;
			}
		}

		return false;
	} // resource_is_indexable

	void indexer::schedule_metadata_purge_for_recursive_rm_object(const std::string& logical_path,
	                                                              const nlohmann::json& recurse_info)
	{
		schedule_policy_event_for_object(
			/*policy_name, */ "irods_policy_recursive_rm_object_by_path",
			logical_path,
			/*_user_name,*/ "",
			EMPTY_RESOURCE_NAME,
			/* _indexer, */ "elasticsearch",
			recurse_info.dump(),
			/* _index_type, */ "metadata",
			generate_delay_execution_parameters());
	}

	// - Starting at _collection_name , recurse over every sub-element of the tree
	// - (including data objects and collections and starting with the root).
	// - Call schedule_policy_event_for_object for every object or collection
	void indexer::schedule_policy_events_for_collection(const std::string& _operation_type,
	                                                    const std::string& _collection_name,
	                                                    const std::string& _user_name,
	                                                    const std::string& _indexer,
	                                                    const std::string& _index_name,
	                                                    const std::string& _index_type)
	{
		namespace fs = irods::experimental::filesystem;
		namespace fsvr = irods::experimental::filesystem::server;
		using fsp = fs::path;
		rsComm_t& comm = *rei_->rsComm;

		const auto indexing_resources = get_indexing_resource_names();
		const auto policy_name = operation_and_index_types_to_policy_name(_operation_type, _index_type);
		fsp start_path{_collection_name};

		int job_limit = config_.job_limit;
		int job_max = std::numeric_limits<decltype(job_limit)>::max();

		if (job_limit <= 0) {
			job_limit = job_max; // 0 is the advertised default
			if (job_limit < 0) { // negative value could result from a value > INT_MAX
				irods::log(LOG_WARNING,
				           fmt::format(R"Qu(Parameter "job_limit_per_collection_indexing_operation" is )Qu"
				                       "too large or negative, clipped to: {}",
				                       job_max));
			}
		}

		auto iter_end = fsvr::recursive_collection_iterator{};
		auto iter = fsvr::recursive_collection_iterator{comm, start_path};

		const std::hash<std::string> string_hasher;
		const auto key1 = string_hasher(_collection_name);
		const auto key2 = string_hasher(config_.instance_name);

		const std::string unique_key = std::to_string(key1) + "-" + std::to_string(key2);

		static const auto double_quote = R"(")"s;
		const auto JOB_QUERY_STRING =
			fmt::format(R"(SELECT count(RULE_EXEC_ID) WHERE RULE_EXEC_NAME like '%"{}"%')", unique_key);
		const auto LOW_WATER_MARK{job_limit / 2};
		auto n_jobs{job_limit * 0U};

		namespace ix = irods::experimental;
		namespace ixm = ix::metadata;
		using entity_type = ix::entity::entity_type;

		ixm::avu md_flag{config_.flag, __func__};

		auto set_flag = (config_.collection_test_flag != "");
		if (set_flag) {
			ixm::set(comm, md_flag, entity_type::collection, config_.collection_test_flag);
		}

		irods::at_scope_exit at_exit_{[&] {
			if (set_flag) {
				ixm::remove(comm, md_flag, entity_type::collection, config_.collection_test_flag);
			}
		}};

		struct query_failed : public std::runtime_error
		{
			query_failed(const std::string& e = "Query failed to fetch # of jobs active")
				: std::runtime_error{e}
			{
			}
		};

		struct job_limit_precision : public std::runtime_error
		{
			job_limit_precision(const std::string& e = "Job Limits may not exceed 32-bit unsigned integer precision")
				: std::runtime_error{e}
			{
			}
		};

		for (auto path = start_path;; ++iter) {
			const auto s = fsvr::status(comm, path);
			bool is_collection = fsvr::is_collection(s);
			bool is_data_object = fsvr::is_data_object(s);
			if (is_data_object || is_collection) {
				try {
					std::string resc_name;
					if (is_data_object) {
						resc_name = get_indexing_resource_name_for_object(path.string(), indexing_resources);
					}

					if (job_limit > 0 && n_jobs >= job_limit) {
						// The job limit parameter should be a large number, in the thousands or more perhaps, but
						// small enough so that indexing your largest collections doesn't fill up all of virtual
						// memory.
						while (true) {
							query<rsComm_t> qobj{comm_, JOB_QUERY_STRING, 1};
							for (const auto& row : qobj) {
								auto count = std::stol(row[0]);
								if (count > job_max) {
									throw job_limit_precision{};
								}
								n_jobs = count;
								break;
							}

							// The approach to throttling is simply to wait until the number of delayed tasks falls
							// down to the LOW_WATER_MARK and then exit the wait loop to fill up the task queue
							// again. Because we're already in a delayed task, this does not impact the plugin's
							// response time.
							if (n_jobs > LOW_WATER_MARK) {
								sleep(1);
							}
							else {
								break;
							}
						}
					}

					if (!(is_collection && _index_type == "full_text")) {
						schedule_policy_event_for_object(policy_name,
						                                 path.string(),
						                                 _user_name,
						                                 EMPTY_RESOURCE_NAME,
						                                 _indexer,
						                                 _index_name,
						                                 _index_type,
						                                 generate_delay_execution_parameters(),
						                                 {},
						                                 {},
						                                 {},
						                                 {{"job_category_tag", unique_key}});

						++n_jobs;
					}
				}
				catch (const exception& _e) {
					rodsLog(LOG_ERROR,
					        "failed to find indexing resource (error code=[%ld]) for object [%s]",
					        static_cast<long>(_e.code()),
					        path.string().c_str());
				}
				catch (const std::runtime_error& e) {
					irods::log(LOG_ERROR, fmt::format("Abort indexing collection: {}", e.what()));
					break;
				}

				if (iter != iter_end) {
					path = iter->path();
				}
				else {
					break;
				}
			}
		}
	}

	/*
	== ////  void indexer::schedule_{INDEX-TYPE}_{EVENT}_event ////
	==  for combinations of INDEX-TYPE => ( full_text, metadata )
	==                  and EVENT      => ( indexing, purge )
	==
	==  Calls schedule_policy_events_given_object_path(...) for the target path
	*/
	void indexer::schedule_full_text_indexing_event(const std::string& _object_path,
	                                                const std::string& _user_name,
	                                                const std::string& _source_resource)
	{
		schedule_policy_events_given_object_path(irods::indexing::operation_type::index,
		                                         irods::indexing::index_type::full_text,
		                                         _object_path,
		                                         _user_name,
		                                         _source_resource);
	} // schedule_full_text_indexing_event

	void indexer::schedule_full_text_purge_event(const std::string& _object_path, const std::string& _user_name)
	{
		schedule_policy_events_given_object_path(
			irods::indexing::operation_type::purge, irods::indexing::index_type::full_text, _object_path, _user_name);
	} // schedule_full_text_purge_event

	void indexer::schedule_metadata_indexing_event(const std::string& _object_path,
	                                               const std::string& _user_name,
	                                               const std::string& _attribute,
	                                               const std::string& _value,
	                                               const std::string& _units)
	{
		schedule_policy_events_given_object_path(irods::indexing::operation_type::index,
		                                         irods::indexing::index_type::metadata,
		                                         _object_path,
		                                         _user_name,
		                                         EMPTY_RESOURCE_NAME,
		                                         _attribute,
		                                         _value,
		                                         _units);
	} // schedule_metadata_indexing_event

	void indexer::schedule_metadata_purge_event(const std::string& _object_path,
	                                            const std::string& _user_name,
	                                            const std::string& _attribute,
	                                            const std::string& _value,
	                                            const std::string& _units,
	                                            const std::string& opt_id)
	{
		// "opt_id" is non-empty if an ID for the deleted object or collection is needed in the task.
		schedule_policy_events_given_object_path(irods::indexing::operation_type::purge,
		                                         irods::indexing::index_type::metadata,
		                                         _object_path,
		                                         _user_name,
		                                         EMPTY_RESOURCE_NAME,
		                                         _attribute,
		                                         _value,
		                                         _units,
		                                         opt_id);
	} // schedule_metadata_purge_event

	// - Given an object path (data object or collection) and an operation type,
	// - ascend collection hierarchy to find the indices for which policy events
	// - must be scheduled on the given object path
	void indexer::schedule_policy_events_given_object_path(const std::string& _operation_type,
	                                                       const std::string& _index_type,
	                                                       const std::string& _object_path,
	                                                       const std::string& _user_name,
	                                                       const std::string& _source_resource,
	                                                       const std::string& _attribute,
	                                                       const std::string& _value,
	                                                       const std::string& _units,
	                                                       const std::string& _opt_ID)
	{
		using fsp = irods::experimental::filesystem::path;
		namespace fsvr = irods::experimental::filesystem::server;
		auto is_data_object = false;
		fsp full_path{_object_path};

		const auto s = fsvr::status(*rei_->rsComm, full_path);
		if (fsvr::is_data_object(s)) {
			is_data_object = true;
			const auto indexing_resources = get_indexing_resource_names();
			if (!resource_is_indexable(_source_resource, indexing_resources)) {
				rodsLog(LOG_ERROR,
				        "resource [%s] is not indexable for object [%s]",
				        _source_resource.c_str(),
				        _object_path.c_str());
				return;
			}
		}

		std::vector<std::string> processed_indicies;
		auto coll = (is_data_object ? full_path.parent_path() : full_path);
		while (!coll.empty()) {
			try {
				auto metadata = get_metadata_for_collection(coll.string(), config_.index);
				for (const auto& row : metadata) {
					const auto& indexer_string = row.first;
					const auto& indexer = row.second;
					std::string index_name, index_type;
					std::tie(index_name, index_type) = parse_indexer_string(indexer_string);
					if (_index_type == index_type) {
						auto itr = std::find(
							std::begin(processed_indicies), std::end(processed_indicies), index_name + index_type);
						if (itr != std::end(processed_indicies)) {
							continue;
						}
						const auto policy_name = operation_and_index_types_to_policy_name(_operation_type, _index_type);
						schedule_policy_event_for_object(policy_name,
						                                 _object_path,
						                                 _user_name,
						                                 _source_resource,
						                                 indexer,
						                                 index_name,
						                                 index_type,
						                                 generate_delay_execution_parameters(),
						                                 _attribute,
						                                 _value,
						                                 _units,
						                                 {{"_obj_optional_ID", _opt_ID}} // _extra_options
						);
						processed_indicies.push_back(index_name + index_type);
					}
				} // for row
			}
			catch (const irods::exception&) {
			}

			if (0 == coll.compare(coll.root_collection())) {
				break;
			}

			coll = coll.parent_path();
		}
	} // schedule_policy_events_given_object_path

	std::string indexer::generate_delay_execution_parameters()
	{
		std::string params{config_.delay_parameters + "<INST_NAME>" + config_.instance_name + "</INST_NAME>"};

		int min_time = config_.minimum_delay_time <= 0 ? 1 : config_.minimum_delay_time;
		int max_time = config_.maximum_delay_time <= 0 ? 30 : config_.maximum_delay_time;

		std::string sleep_time{"1"};
		try {
			std::random_device rd;
			std::mt19937 gen(rd());
			std::uniform_int_distribution<> dis(min_time, max_time);
			sleep_time = boost::lexical_cast<std::string>(dis(gen));
		}
		catch (const boost::bad_lexical_cast&) {
		}

		params += "<PLUSET>" + sleep_time + "s</PLUSET>";

		rodsLog(config_.log_level,
		        "irods::storage_tiering :: delay params min [%d] max [%d] computed [%s]",
		        min_time,
		        max_time,
		        params.c_str());

		return params;

	} // generate_delay_execution_parameters

	void indexer::get_metadata_for_data_object(const std::string& _meta_attr_name,
	                                           const std::string& _object_path,
	                                           std::string& _value,
	                                           std::string& _unit)
	{
		boost::filesystem::path p{_object_path};
		std::string coll_name = p.parent_path().string();
		std::string data_name = p.filename().string();

		std::string query_str{boost::str(boost::format("SELECT META_DATA_ATTR_VALUE WHERE META_DATA_ATTR_NAME = "
		                                               "'%s' and DATA_NAME = '%s' AND COLL_NAME = '%s'") %
		                                 _meta_attr_name % data_name % coll_name)};
		query<rsComm_t> qobj{comm_, query_str, 1};
		if (qobj.size() == 0) {
			THROW(
				CAT_NO_ROWS_FOUND,
				boost::format("no results found for object [%s] with attribute [%s]") % _object_path % _meta_attr_name);
		}

		_value = qobj.front()[0];
		_unit = qobj.front()[1];
	} // get_metadata_for_data_object

	indexer::metadata_results indexer::get_metadata_for_collection(const std::string& _collection,
	                                                               const std::string& _meta_attr_name)
	{
		std::string query_str{boost::str(boost::format("SELECT META_COLL_ATTR_VALUE, META_COLL_ATTR_UNITS WHERE "
		                                               "META_COLL_ATTR_NAME = '%s' and COLL_NAME = '%s'") %
		                                 _meta_attr_name % _collection)};
		query<rsComm_t> qobj{comm_, query_str};
		if (qobj.size() == 0) {
			THROW(CAT_NO_ROWS_FOUND,
			      boost::format("no results found for collection [%s] with attribute [%s]") % _collection %
			          _meta_attr_name);
		}

		metadata_results ret_val;
		for (const auto& row : qobj) {
			ret_val.push_back(std::make_pair(row[0], row[1]));
		}

		return ret_val;
	} // get_metadata_for_collection

	void indexer::schedule_policy_event_for_object(const std::string& _event,
	                                               const std::string& _object_path,
	                                               const std::string& _user_name,
	                                               const std::string& _source_resource,
	                                               const std::string& _indexer,
	                                               const std::string& _index_name,
	                                               const std::string& _index_type,
	                                               const std::string& _data_movement_params,
	                                               const std::string& _attribute,
	                                               const std::string& _value,
	                                               const std::string& _units,
	                                               const nlohmann::json& _extra_options)
	{
		using json = nlohmann::json;

		const auto& [id_bool, obj_optional_ID] = kws_get<std::string>(_extra_options, "_obj_optional_ID");
		const auto& [tag_bool, job_category_tag] = kws_get<std::string>(_extra_options, "job_category_tag");

		json rule_obj;
		rule_obj["rule-engine-operation"] = _event;
		rule_obj["rule-engine-instance-name"] = config_.instance_name;
		rule_obj["object-path"] = id_bool && !(*obj_optional_ID).empty() ? *obj_optional_ID : _object_path;
		rule_obj["user-name"] = _user_name;
		rule_obj["indexer"] = _indexer;
		rule_obj["index-name"] = _index_name;
		rule_obj["index-type"] = _index_type;
		rule_obj["source-resource"] = _source_resource;
		rule_obj["attribute"] = _attribute;
		rule_obj["value"] = _value;
		rule_obj["units"] = _units;
		if (tag_bool && job_category_tag) {
			rule_obj["job-category-tag"] = *job_category_tag;
		}

		const auto delay_err = _delayExec(rule_obj.dump().c_str(), "", _data_movement_params.c_str(), rei_);
		if (delay_err < 0) {
			THROW(delay_err,
			      boost::format("queue indexing event failed for object [%s] indexer [%s] type [%s]") % _object_path %
			          _indexer % _index_type);
		}

		rodsLog(config_.log_level,
		        "irods::indexing::indexer indexing object [%s] with [%s] type [%s]",
		        _object_path.c_str(),
		        _indexer.c_str(),
		        _index_type.c_str());

	} // schedule_policy_event_for_object
} // namespace irods::indexing
