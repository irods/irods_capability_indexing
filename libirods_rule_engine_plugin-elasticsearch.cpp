
#define IRODS_IO_TRANSPORT_ENABLE_SERVER_SIDE_API

#include "irods_query.hpp"
#include "irods_re_plugin.hpp"
#include "irods_re_ruleexistshelper.hpp"
#include "utilities.hpp"
#include "plugin_specific_configuration.hpp"
#include "configuration.hpp"
#include "dstream.hpp"
#include "rsModAVUMetadata.hpp"
#include "irods_hasher_factory.hpp"
#include "MD5Strategy.hpp"

#include "transport/default_transport.hpp"
#include "filesystem.hpp"

#include "cpr/response.h"
#include "elasticlient/client.h"
#include "elasticlient/bulk.h"
#include "elasticlient/logging.h"

#include <boost/any.hpp>
#include <boost/format.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/archive/iterators/base64_from_binary.hpp>
#include <boost/archive/iterators/transform_width.hpp>
#include <boost/archive/iterators/ostream_iterator.hpp>

#include <string>
#include <sstream>
#include <algorithm>

namespace {
    struct configuration : irods::indexing::configuration {
        std::vector<std::string> hosts_;
        int                      bulk_count_{10};
        int                      read_size_{4194304};
        configuration(const std::string& _instance_name) :
            irods::indexing::configuration(_instance_name) {
            try {
                auto cfg = irods::indexing::get_plugin_specific_configuration(_instance_name);
                if(cfg.find("hosts") != cfg.end()) {
                    std::vector<boost::any> host_list = boost::any_cast<std::vector<boost::any>>(cfg.at("hosts"));
                    for( auto& i : host_list) {
                        hosts_.push_back(boost::any_cast<std::string>(i));
                    }
                }

                if(cfg.find("bulk_count") != cfg.end()) {
                    bulk_count_ = boost::any_cast<int>(cfg.at("bulk_count"));
                }

                if(cfg.find("read_size") != cfg.end()) {
                    bulk_count_ = boost::any_cast<int>(cfg.at("read_size"));
                }
            }
            catch(const boost::bad_any_cast& _e) {
                THROW(
                    INVALID_ANY_CAST,
                    _e.what());
            }
        }// ctor
    }; // configuration

    std::unique_ptr<configuration> config;
    std::string object_index_policy;
    std::string object_purge_policy;
    std::string metadata_index_policy;
    std::string metadata_purge_policy;

    void apply_document_type_policy(
        ruleExecInfo_t*    _rei,
        const std::string& _object_path,
        const std::string& _source_resource,
        std::string*       _document_type) {

        std::list<boost::any> args;
        args.push_back(boost::any(_object_path));
        args.push_back(boost::any(_source_resource));
        args.push_back(boost::any(_document_type));
        std::string policy_name = irods::indexing::policy::compose_policy_name(
                                  irods::indexing::policy::prefix,
                                  "document_type_elastic");
        irods::indexing::invoke_policy(_rei, policy_name, args);

    } // apply_document_type_policy

    void log_fcn(elasticlient::LogLevel, const std::string& _msg) {
        rodsLog(LOG_ERROR, "ELASTICLIENT :: [%s]", _msg.c_str());
    } // log_fcn

    std::string generate_id() {
        using namespace boost::archive::iterators;
        std::stringstream os;
        typedef
            base64_from_binary< // convert binary values to base64 characters
                transform_width<// retrieve 6 bit integers from a sequence of 8 bit bytes
                    const char *,
                    6,
                    8
                >
            >
            base64_text; // compose all the above operations in to a new iterator

        boost::uuids::uuid uuid{boost::uuids::random_generator()()};
        std::string uuid_str = boost::uuids::to_string(uuid);
        std::copy(
            base64_text(uuid_str.c_str()),
            base64_text(uuid_str.c_str() + uuid_str.size()),
            ostream_iterator<char>(os));

        return os.str();
    } // generate_id

    std::string get_object_index_id(
        ruleExecInfo_t*    _rei,
        const std::string& _object_path) {
        boost::filesystem::path p{_object_path};
        std::string coll_name = p.parent_path().string();
        std::string data_name = p.filename().string();
        std::string query_str {
            boost::str(
                boost::format("SELECT DATA_ID WHERE DATA_NAME = '%s' AND COLL_NAME = '%s'")
                    % data_name
                    % coll_name) };

        try {
            irods::query<rsComm_t> qobj{_rei->rsComm, query_str, 1};
            if(qobj.size() > 0) {
                return qobj.front()[0];
            }
            THROW(
                CAT_NO_ROWS_FOUND,
                boost::format("failed to get object id for [%s]")
                % _object_path);
        }
        catch(const irods::exception& _e) {
            THROW(
                CAT_NO_ROWS_FOUND,
                boost::format("failed to get object id for [%s]")
                % _object_path);
        }

    } // get_object_index_id

    void update_object_metadata(
        ruleExecInfo_t*    _rei,
        const std::string& _object_path,
        const std::string& _attribute,
        const std::string& _value,
        const std::string& _units ) {
        modAVUMetadataInp_t set_op{
            .arg0 = "set",
            .arg1 = "-d",
            .arg2 = const_cast<char*>(_object_path.c_str()),
            .arg3 = const_cast<char*>(_attribute.c_str()),
            .arg4 = const_cast<char*>(_value.c_str()),
            .arg5 = const_cast<char*>(_units.c_str())};

        auto status = rsModAVUMetadata(_rei->rsComm, &set_op);
        if(status < 0) {
            THROW(
                status,
                boost::format("failed to update object [%s] metadata")
                % _object_path);
        }
    } // update_object_metadata

    void invoke_indexing_event_full_text(
        ruleExecInfo_t*    _rei,
        const std::string& _object_path,
        const std::string& _source_resource,
        const std::string& _index_name) {

        try {
            std::string doc_type{"text"};
            apply_document_type_policy(
                _rei,
                _object_path,
                _source_resource,
                &doc_type);

            const long read_size{config->read_size_};
            const int bulk_count{config->bulk_count_};
            const std::string object_id{get_object_index_id(_rei, _object_path)};

            std::shared_ptr<elasticlient::Client> client =
                std::make_shared<elasticlient::Client>(
                    config->hosts_);
            elasticlient::Bulk bulkIndexer(client);
            elasticlient::SameIndexBulkData bulk(_index_name, bulk_count);

            char read_buff[read_size];
            irods::experimental::io::server::basic_transport<char> xport(*_rei->rsComm);
            irods::experimental::io::idstream ds{xport, _object_path};

            int chunk_counter{0};
            bool need_final_perform{false};
            while(ds) {
                ds.read(read_buff, read_size);
                std::string data{read_buff};

                // filter out new line characters
                data.erase(
                    std::remove_if(
                        data.begin(),
                        data.end(),
                    [](wchar_t c) {return (std::iscntrl(c) || c == '"' || c == '\'' || c == '\\');}),
                data.end());

                std::string index_id{
                                boost::str(
                                boost::format(
                                "%s_%d")
                                % object_id
                                % chunk_counter)};
                ++chunk_counter;

                std::string payload{
                                boost::str(
                                boost::format(
                                "{ \"object_path\" : \"%s\", \"data\" : \"%s\" }")
                                % _object_path
                                % data)};

                need_final_perform = true;
                bool done = bulk.indexDocument(doc_type, index_id, payload.data());
                if(done) {
                    need_final_perform = false;
                    // have reached bulk_count chunks
                    auto error_count = bulkIndexer.perform(bulk);
                    if(error_count > 0) {
                        rodsLog(
                            LOG_ERROR,
                            "Encountered %d errors when indexing [%s]",
                            error_count,
                            _object_path.c_str());
                    }
                    bulk.clear();
                }
            } // while

            if(need_final_perform) {
                auto error_count = bulkIndexer.perform(bulk);
                if(error_count > 0) {
                    rodsLog(
                        LOG_ERROR,
                        "Encountered %d errors when indexing [%s]",
                        error_count,
                        _object_path.c_str());
                }
                bulk.clear();
            }
        }
        catch(const std::runtime_error& _e) {
            rodsLog(
                LOG_ERROR,
                "Exception [%s]",
                _e.what());
            THROW(
                SYS_INTERNAL_ERR,
                _e.what());
        }
        catch(const std::exception& _e) {
            rodsLog(
                LOG_ERROR,
                "Exception [%s]",
                _e.what());
            THROW(
                SYS_INTERNAL_ERR,
                _e.what());
        }
    } // invoke_indexing_event_full_text

    void invoke_purge_event_full_text(
        ruleExecInfo_t*    _rei,
        const std::string& _object_path,
        const std::string& _source_resource,
        const std::string& _index_name) {

        try {
            std::string doc_type{"text"};
            apply_document_type_policy(
                _rei,
                _object_path,
                _source_resource,
                &doc_type);

            const long read_size{config->read_size_};
            const int bulk_count{config->bulk_count_};
            const std::string object_id{get_object_index_id(_rei, _object_path)};
            elasticlient::Client client{config->hosts_};

            int chunk_counter{0};

            bool done{false};
            while(!done) {
                std::string index_id{
                                boost::str(
                                boost::format(
                                "%s_%d")
                                % object_id
                                % chunk_counter)};
                ++chunk_counter;
                const cpr::Response response = client.remove(_index_name, doc_type, index_id);
                if(response.status_code != 200) {
                    done = true;
                    if(response.status_code == 404) { // meaningful for logging
                        rodsLog (LOG_NOTICE, boost::str(boost::format("elasticlient 404: no index entry for chunk (%d) of object_id '%d' "
                                                                      "in index '%s'") % chunk_counter % object_id % _index_name).c_str());
                    }
                }
            } // while
        }
        catch(const std::runtime_error& _e) {
            rodsLog(
                LOG_ERROR,
                "Exception [%s]",
                _e.what());
            THROW(
                SYS_INTERNAL_ERR,
                _e.what());
        }
        catch(const irods::exception& _e) {
            if (_e.code() == CAT_NO_ROWS_FOUND) {
                return;
            }
            THROW(
                SYS_INTERNAL_ERR,
                _e.what());
        }
        catch(const std::exception& _e) {
            rodsLog(
                LOG_ERROR,
                "Exception [%s]",
                _e.what());
            THROW(
                SYS_INTERNAL_ERR,
                _e.what());
        }
    } // invoke_purge_event_full_text

    std::string get_metadata_index_id(
        const std::string& _index_id,
        const std::string& _attribute,
        const std::string& _value,
        const std::string& _units) {

        std::string str = _attribute +
                          _value +
                          _units;
        irods::Hasher hasher;
        irods::getHasher( irods::MD5_NAME, hasher );
        hasher.update(str);

        std::string digest;
        hasher.digest(digest);

        return _index_id + irods::indexing::indexer_separator + digest;

    } // get_metadata_index_id

    void invoke_indexing_event_metadata(
        ruleExecInfo_t*    _rei,
        const std::string& _object_path,
        const std::string& _attribute,
        const std::string& _value,
        const std::string& _unit,
        const std::string& _index_name) {

        try {
            elasticlient::Client client{config->hosts_};
            const std::string md_index_id{
                                  get_metadata_index_id(
                                      get_object_index_id(
                                          _rei,
                                          _object_path),
                                      _attribute,
                                      _value,
                                      _unit)};
            std::string payload{
                            boost::str(
                            boost::format(
                            "{ \"object_path\":\"%s\", \"attribute\":\"%s\", \"value\":\"%s\", \"units\":\"%s\" }")
                            % _object_path
                            % _attribute
                            % _value
                            % _unit)} ;
            const cpr::Response response = client.index(_index_name, "text", md_index_id, payload);
            if(response.status_code != 200 && response.status_code != 201) {
                THROW(
                    SYS_INTERNAL_ERR,
                    boost::format("failed to index metadata [%s] [%s] [%s] for [%s] code [%d] message [%s]")
                    % _attribute
                    % _value
                    % _unit
                    % _object_path
                    % response.status_code
                    % response.text);
            }
        }
        catch(const std::runtime_error& _e) {
            rodsLog(
                LOG_ERROR,
                "Exception [%s]",
                _e.what());
            THROW(
                SYS_INTERNAL_ERR,
                _e.what());
        }
        catch(const std::exception& _e) {
            rodsLog(
                LOG_ERROR,
                "Exception [%s]",
                _e.what());
            THROW(
                SYS_INTERNAL_ERR,
                _e.what());
        }
    } // invoke_indexing_event_metadata

    void invoke_purge_event_metadata(
        ruleExecInfo_t*    _rei,
        const std::string& _object_path,
        const std::string& _attribute,
        const std::string& _value,
        const std::string& _unit,
        const std::string& _index_name) {

        try {
            elasticlient::Client client{config->hosts_};
            const std::string md_index_id{
                                  get_metadata_index_id(
                                      get_object_index_id(
                                          _rei,
                                          _object_path),
                                      _attribute,
                                      _value,
                                      _unit)};
            const cpr::Response response = client.remove(_index_name, "text", md_index_id);
            switch(response.status_code) {
                // either the index has been deleted, or the AVU was cleared unexpectedly
                case 404:
                    rodsLog (LOG_NOTICE, boost::str(boost::format("elasticlient 404: no index entry for AVU (%s,%s,%s) on object '%s' in "
                                                        "index '%s'") % _attribute % _value % _unit % _object_path % _index_name).c_str());
                    break;
                // routinely expected return codes ( not logged ):
                case 200: break;
                case 201: break;
                // unexpected return codes:
                default:
                    THROW(
                        SYS_INTERNAL_ERR,
                        boost::format("failed to index metadata [%s] [%s] [%s] for [%s] code [%d] message [%s]")
                        % _attribute
                        % _value
                        % _unit
                        % _object_path
                        % response.status_code
                        % response.text);
            }
        }
        catch(const std::runtime_error& _e) {
            rodsLog(
                LOG_ERROR,
                "Exception [%s]",
                _e.what());
            THROW(
                SYS_INTERNAL_ERR,
                _e.what());
        }
        catch(const std::exception& _e) {
            rodsLog(
                LOG_ERROR,
                "Exception [%s]",
                _e.what());
            THROW(
                SYS_INTERNAL_ERR,
                _e.what());
        }

    } // invoke_purge_event_metadata

} // namespace

irods::error start(
    irods::default_re_ctx&,
    const std::string& _instance_name ) {
    RuleExistsHelper::Instance()->registerRuleRegex("irods_policy_.*");
    config = std::make_unique<configuration>(_instance_name);
    object_index_policy = irods::indexing::policy::compose_policy_name(
                               irods::indexing::policy::object::index,
                               "elasticsearch");
    object_purge_policy = irods::indexing::policy::compose_policy_name(
                               irods::indexing::policy::object::purge,
                               "elasticsearch");
    metadata_index_policy = irods::indexing::policy::compose_policy_name(
                               irods::indexing::policy::metadata::index,
                               "elasticsearch");
    metadata_purge_policy = irods::indexing::policy::compose_policy_name(
                               irods::indexing::policy::metadata::purge,
                               "elasticsearch");

    elasticlient::setLogFunction(log_fcn);
    return SUCCESS();
}

irods::error stop(
    irods::default_re_ctx&,
    const std::string& ) {
    return SUCCESS();
}

irods::error rule_exists(
    irods::default_re_ctx&,
    const std::string& _rn,
    bool&              _ret) {
    _ret = object_index_policy   == _rn ||
           object_purge_policy   == _rn ||
           metadata_index_policy == _rn ||
           metadata_purge_policy == _rn;
    return SUCCESS();
}

irods::error list_rules(
    irods::default_re_ctx&,
    std::vector<std::string>& _rules) {
    _rules.push_back(object_index_policy);
    _rules.push_back(object_purge_policy);
    _rules.push_back(metadata_index_policy);
    _rules.push_back(metadata_purge_policy);
    return SUCCESS();
}

irods::error exec_rule(
    irods::default_re_ctx&,
    const std::string&     _rn,
    std::list<boost::any>& _args,
    irods::callback        _eff_hdlr) {
    ruleExecInfo_t* rei{};
    const auto err = _eff_hdlr("unsafe_ms_ctx", &rei);
    if(!err.ok()) {
        return err;
    }

    try {
        if(_rn == object_index_policy) {
            auto it = _args.begin();
            const std::string object_path{ boost::any_cast<std::string>(*it) }; ++it;
            const std::string source_resource{ boost::any_cast<std::string>(*it) }; ++it;
            const std::string index_name{ boost::any_cast<std::string>(*it) }; ++it;

            invoke_indexing_event_full_text(
                rei,
                object_path,
                source_resource,
                index_name);
        }
        else if(_rn == object_purge_policy) {
            auto it = _args.begin();
            const std::string object_path{ boost::any_cast<std::string>(*it) }; ++it;
            const std::string source_resource{ boost::any_cast<std::string>(*it) }; ++it;
            const std::string index_name{ boost::any_cast<std::string>(*it) }; ++it;

            invoke_purge_event_full_text(
                rei,
                object_path,
                source_resource,
                index_name);
        }
        else if(_rn == metadata_index_policy) {
            auto it = _args.begin();
            const std::string object_path{ boost::any_cast<std::string>(*it) }; ++it;
            const std::string attribute{ boost::any_cast<std::string>(*it) }; ++it;
            const std::string value{ boost::any_cast<std::string>(*it) }; ++it;
            const std::string unit{ boost::any_cast<std::string>(*it) }; ++it;
            const std::string index_name{ boost::any_cast<std::string>(*it) }; ++it;

            invoke_indexing_event_metadata(
                rei,
                object_path,
                attribute,
                value,
                unit,
                index_name);
        }
        else if(_rn == metadata_purge_policy) {
            auto it = _args.begin();
            const std::string object_path{ boost::any_cast<std::string>(*it) }; ++it;
            const std::string attribute{ boost::any_cast<std::string>(*it) }; ++it;
            const std::string value{ boost::any_cast<std::string>(*it) }; ++it;
            const std::string unit{ boost::any_cast<std::string>(*it) }; ++it;
            const std::string index_name{ boost::any_cast<std::string>(*it) }; ++it;

            invoke_purge_event_metadata(
                rei,
                object_path,
                attribute,
                value,
                unit,
                index_name);

        }
        else {
            return ERROR(
                    SYS_NOT_SUPPORTED,
                    _rn);
        }
    }
    catch(const std::invalid_argument& _e) {
        irods::indexing::exception_to_rerror(
            SYS_NOT_SUPPORTED,
            _e.what(),
            rei->rsComm->rError);
        return ERROR(
                   SYS_NOT_SUPPORTED,
                   _e.what());
    }
    catch(const boost::bad_any_cast& _e) {
        irods::indexing::exception_to_rerror(
            INVALID_ANY_CAST,
            _e.what(),
            rei->rsComm->rError);
        return ERROR(
                   SYS_NOT_SUPPORTED,
                   _e.what());
    }
    catch(const irods::exception& _e) {
        irods::indexing::exception_to_rerror(
            _e,
            rei->rsComm->rError);
        return irods::error(_e);
    }

    return err;

} // exec_rule

irods::error exec_rule_text(
    irods::default_re_ctx&,
    const std::string&,
    msParamArray_t*,
    const std::string&,
    irods::callback ) {
    return ERROR(
            RULE_ENGINE_CONTINUE,
            "exec_rule_text is not supported");
} // exec_rule_text

irods::error exec_rule_expression(
    irods::default_re_ctx&,
    const std::string&,
    msParamArray_t*,
    irods::callback) {
    return ERROR(
            RULE_ENGINE_CONTINUE,
            "exec_rule_expression is not supported");
} // exec_rule_expression

extern "C"
irods::pluggable_rule_engine<irods::default_re_ctx>* plugin_factory(
    const std::string& _inst_name,
    const std::string& _context ) {
    irods::pluggable_rule_engine<irods::default_re_ctx>* re = 
        new irods::pluggable_rule_engine<irods::default_re_ctx>(
                _inst_name,
                _context);
    re->add_operation<
        irods::default_re_ctx&,
        const std::string&>(
            "start",
            std::function<
                irods::error(
                    irods::default_re_ctx&,
                    const std::string&)>(start));
    re->add_operation<
        irods::default_re_ctx&,
        const std::string&>(
            "stop",
            std::function<
                irods::error(
                    irods::default_re_ctx&,
                    const std::string&)>(stop));
    re->add_operation<
        irods::default_re_ctx&,
        const std::string&,
        bool&>(
            "rule_exists",
            std::function<
                irods::error(
                    irods::default_re_ctx&,
                    const std::string&,
                    bool&)>(rule_exists));
    re->add_operation<
        irods::default_re_ctx&,
        std::vector<std::string>&>(
            "list_rules",
            std::function<
                irods::error(
                    irods::default_re_ctx&,
                    std::vector<std::string>&)>(list_rules));
    re->add_operation<
        irods::default_re_ctx&,
        const std::string&,
        std::list<boost::any>&,
        irods::callback>(
            "exec_rule",
            std::function<
                irods::error(
                    irods::default_re_ctx&,
                    const std::string&,
                    std::list<boost::any>&,
                    irods::callback)>(exec_rule));
    re->add_operation<
        irods::default_re_ctx&,
        const std::string&,
        msParamArray_t*,
        const std::string&,
        irods::callback>(
            "exec_rule_text",
            std::function<
                irods::error(
                    irods::default_re_ctx&,
                    const std::string&,
                    msParamArray_t*,
                    const std::string&,
                    irods::callback)>(exec_rule_text));

    re->add_operation<
        irods::default_re_ctx&,
        const std::string&,
        msParamArray_t*,
        irods::callback>(
            "exec_rule_expression",
            std::function<
                irods::error(
                    irods::default_re_ctx&,
                    const std::string&,
                    msParamArray_t*,
                    irods::callback)>(exec_rule_expression));
    return re;

} // plugin_factory




