
// =-=-=-=-=-=-=-
// irods includes
#define IRODS_QUERY_ENABLE_SERVER_SIDE_API
#include <irods/irods_query.hpp>
#include <irods/irods_re_plugin.hpp>
#include <irods/irods_re_ruleexistshelper.hpp>
#include <irods/irods_hierarchy_parser.hpp>
#include <irods/irods_resource_backport.hpp>
#include <irods/rsModAVUMetadata.hpp>
#define IRODS_FILESYSTEM_ENABLE_SERVER_SIDE_API

#include <irods/filesystem.hpp>

#include <irods/irods_log.hpp>

#include "utilities.hpp"
#include "indexing_utilities.hpp"

#undef LIST

// =-=-=-=-=-=-=-
// stl includes
#include <iostream>
#include <sstream>
#include <set>
#include <vector>
#include <tuple>
#include <map>
#include <string>
#include <fmt/format.h>

#include <iterator>
#include <algorithm>
#include <typeinfo>

// =-=-=-=-=-=-=-
// boost includes
#include <boost/any.hpp>
#include <boost/exception/all.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/optional.hpp>

#include <nlohmann/json.hpp>

#include <irods/objDesc.hpp>

using namespace std::string_literals;

extern l1desc_t L1desc[NUM_L1_DESC];


int _delayExec(
    const char *inActionCall,
    const char *recoveryActionCall,
    const char *delayCondition,
    ruleExecInfo_t *rei );

namespace
{
    // For handling of atomic metadata ops

    using metadata_tuple = std::tuple<std::string, std::string, std::string>;
    std::map <std::string, std::set<metadata_tuple>> atomic_metadata_tuples {};
    std::string atomic_metadata_obj_path {};

    // Other objects with visibility in this module only

    std::set<std::string> indices_for_rm_coll;
    bool collection_metadata_is_new = false;
    std::unique_ptr<irods::indexing::configuration>     config;
    std::map<int, std::tuple<std::string, std::string>> opened_objects;

    const char* rm_force_kw = "*"; // default value tested for in "*_post" PEPs

    //-- Collect the AVU indexing indicators from collections in an object's parent
    //-  chain or attached to subcollections (any level deep) of the given object.
    //-  This is a convenient and not too far-reaching superset of the actual set
    //-  of indicators that apply for the deletion of this and all sub objects. Note
    //-  the object is going away, so there is no harm in deleting mentions of it
    //-  and any subobjects from all indices so computed, even if some of them don't
    //-  refer to the object(s).

    auto get_indices_for_delete_by_query (rsComm_t& comm, const std::string &_object_name, const bool recurs) -> std::set<std::string>
    {
        using irods::indexing::parse_indexer_string;

        namespace fs   = irods::experimental::filesystem;
        namespace fsvr = irods::experimental::filesystem::server;
        using     fsp  = fs::path;

        fsp node {_object_name}, up_node {_object_name};

        std::set<std::string> indices;

        auto get_indices_from_collection_AVUs = [&](const std::string& collection){
            irods::query q { &comm, fmt::format("select META_COLL_ATTR_VALUE where "
                                                "META_COLL_ATTR_NAME = '{}' and COLL_NAME = '{}'",
                                                config->index,collection) };
            for (const auto &row : q) {
                std::string index_name = std::get<0>(parse_indexer_string(row[0]));
                indices.insert( index_name );
            }
        };

        while (!up_node.empty()) {
            if (fsvr::is_collection(comm,up_node)) { get_indices_from_collection_AVUs(up_node.string()); }
            up_node = up_node.parent_path();
            if (0 == up_node.compare(node.root_collection())) { break; }
        }

        if (recurs) {
            auto iter_end = fsvr::recursive_collection_iterator{};
            auto iter =  fsvr::recursive_collection_iterator{comm, _object_name};
            for (; iter != iter_end ; ++iter) {
                if (fsvr::is_collection(comm,*iter)) { get_indices_from_collection_AVUs(iter->path().string()); }
            }
        }

        return indices;
    }

    // -=-=-=  Search for objPath, return L1 desc, Resource name
    // -
    // - get_index_and_resource(const dataObjInp_t* _inp)
    // -

    std::tuple<int, std::string>
    get_index_and_resource(const dataObjInp_t* _inp) {
        int l1_idx{};
        dataObjInfo_t* obj_info{};
        for(const auto& l1 : L1desc) {
            if(FD_INUSE != l1.inuseFlag) {
                continue;
            }
            if(!strcmp(l1.dataObjInp->objPath, _inp->objPath)) {
                obj_info = l1.dataObjInfo;
                l1_idx = &l1 - L1desc;
            }
        }

        if(nullptr == obj_info) {
            THROW(
                SYS_INVALID_INPUT_PARAM,
                "no object found");
        }

        std::string resource_name;
        irods::error err = irods::get_resource_property<std::string>(
                               obj_info->rescId,
                               irods::RESOURCE_NAME,
                               resource_name);
        if(!err.ok()) {
            THROW(err.code(), err.result());
        }

        return std::make_tuple(l1_idx, resource_name);
    } // get_object_path_and_resource

#define NULL_PTR_GUARD(x) ((x) == nullptr ? "" : (x))

    // -
    // -=-=-=  For the various PEP's , setup, schedule and/or initiate indexing policy
    // -
    // - apply_indexing_policy (const dataObjInp_t* _inp)
    // -

    void apply_indexing_policy(
        const std::string &    _rn,
        ruleExecInfo_t*        _rei,
        std::list<boost::any>& _args) {
        try {
            std::string object_path;
            std::string source_resource;
            // NOTE:: 3rd parameter is the target
            if("pep_api_data_obj_put_post"  == _rn) {
                auto it = _args.begin();
                std::advance(it, 2);
                if(_args.end() == it) {
                    THROW(
                        SYS_INVALID_INPUT_PARAM,
                        "invalid number of arguments");
                }

                auto obj_inp = boost::any_cast<dataObjInp_t*>(*it);
                object_path = obj_inp->objPath;

                const char* resc_hier = getValByKey(
                                            &obj_inp->condInput,
                                            RESC_HIER_STR_KW);
                if(!resc_hier) {
                    THROW(SYS_INVALID_INPUT_PARAM, "resc hier is null");
                }

                irods::hierarchy_parser parser;
                parser.set_string(resc_hier);
                parser.last_resc(source_resource);

                irods::indexing::indexer idx{_rei, config->instance_name_};
                idx.schedule_full_text_indexing_event(
                    object_path,
                    _rei->rsComm->clientUser.userName,
                    source_resource);

                const char* metadata_included = getValByKey(&obj_inp->condInput, METADATA_INCLUDED_KW);
                if (metadata_included)
                {
                    idx.schedule_metadata_indexing_event(
                        object_path,
                        _rei->rsComm->clientUser.userName, "a", "v", "u"); // "a","v","u" values are not significant; this is
                                                                           // just a trigger to (re-)index all AVUs.  Ref: #117
                }
            }
            else if("pep_api_data_obj_repl_post" == _rn) {
                auto it = _args.begin();
                std::advance(it, 2);
                if(_args.end() == it) {
                    THROW(
                        SYS_INVALID_INPUT_PARAM,
                        "invalid number of arguments");
                }

                auto obj_inp = boost::any_cast<dataObjInp_t*>(*it);
                object_path = obj_inp->objPath;
                const char* resc_hier = getValByKey(
                                            &obj_inp->condInput,
                                            DEST_RESC_HIER_STR_KW);
                if(!resc_hier) {
                    THROW(SYS_INVALID_INPUT_PARAM, "resc hier is null");
                }

                irods::hierarchy_parser parser;
                parser.set_string(resc_hier);
                parser.last_resc(source_resource);

                irods::indexing::indexer idx{_rei, config->instance_name_};
                idx.schedule_full_text_indexing_event(
                    object_path,
                    _rei->rsComm->clientUser.userName,
                    source_resource);
            }
            else if("pep_api_data_obj_open_post"   == _rn ||
                    "pep_api_data_obj_create_post" == _rn) {
                auto it = _args.begin();
                std::advance(it, 2);
                if(_args.end() == it) {
                    THROW(
                        SYS_INVALID_INPUT_PARAM,
                        "invalid number of arguments");
                }

                auto obj_inp = boost::any_cast<dataObjInp_t*>(*it);
                if(obj_inp->openFlags & O_WRONLY || obj_inp->openFlags & O_RDWR) {
                    int l1_idx{};
                    std::string resource_name;
                    try {
                        std::tie(l1_idx, resource_name) = get_index_and_resource(obj_inp);
                        opened_objects[l1_idx] = std::tie(obj_inp->objPath, resource_name);
                    }
                    catch(const irods::exception& _e) {
                        rodsLog(
                           LOG_ERROR,
                           "get_index_and_resource failed for [%s]",
                           obj_inp->objPath);
                    }
                }
            }
            else if("pep_api_data_obj_close_post" == _rn) {
                //TODO :: only for create/write events
                auto it = _args.begin();
                std::advance(it, 2);
                if(_args.end() == it) {
                    THROW(
                        SYS_INVALID_INPUT_PARAM,
                        "invalid number of arguments");
                }

                const auto opened_inp = boost::any_cast<openedDataObjInp_t*>(*it);
                const auto l1_idx = opened_inp->l1descInx;
                if(opened_objects.find(l1_idx) != opened_objects.end()) {
                    std::string object_path, resource_name;
                    std::tie(object_path, resource_name) = opened_objects[l1_idx];
                    irods::indexing::indexer idx{_rei, config->instance_name_};
                    idx.schedule_full_text_indexing_event(
                        object_path,
                        _rei->rsComm->clientUser.userName,
                        resource_name);
                }
            }
            else if("pep_api_mod_avu_metadata_pre" == _rn) {
                auto it = _args.begin();
                std::advance(it, 2);
                if(_args.end() == it) {
                    THROW(
                        SYS_INVALID_INPUT_PARAM,
                        "invalid number of arguments");
                }

                const auto avu_inp = boost::any_cast<modAVUMetadataInp_t*>(*it);
                const std::string attribute{avu_inp->arg3};
                if(config->index != attribute) {
                    return;
                }

                const std::string operation{avu_inp->arg0};
                const std::string type{avu_inp->arg1};
                const std::string object_path{avu_inp->arg2};
                const std::string add{"add"};
                const std::string set{"set"};
                const std::string collection{"-C"};

                irods::indexing::indexer idx{_rei, config->instance_name_};
                if(operation == set || operation == add) {
                    if(type == collection) {
                        // was the added tag an indexing indicator
                        if(config->index == attribute) {
                            // verify that this is not new metadata with a query and set a flag
                            if (!avu_inp->arg3) { THROW( SYS_INVALID_INPUT_PARAM, "empty metadata attribute" ); }
                            if (!avu_inp->arg4) { THROW( SYS_INVALID_INPUT_PARAM, "empty metadata value" ); }
                            collection_metadata_is_new = !idx.metadata_exists_on_collection(
                                                             object_path,
                                                             avu_inp->arg3,
                                                             avu_inp->arg4,
                                                             NULL_PTR_GUARD(avu_inp->arg5));
                        }
                    }
                }
            }
            else if("pep_api_mod_avu_metadata_post" == _rn) {
                auto it = _args.begin();
                std::advance(it, 2);
                if(_args.end() == it) {
                    THROW(
                        SYS_INVALID_INPUT_PARAM,
                        "invalid number of arguments");
                }

                const auto avu_inp = boost::any_cast<modAVUMetadataInp_t*>(*it);
                const std::string operation{avu_inp->arg0};
                const std::string type{avu_inp->arg1};
                const std::string logical_path{avu_inp->arg2};

                if (!avu_inp->arg3) { THROW( SYS_INVALID_INPUT_PARAM, "empty metadata attribute" ); }
                if (!avu_inp->arg4) { THROW( SYS_INVALID_INPUT_PARAM, "empty metadata value" ); }
                const std::string attribute{ avu_inp->arg3 };
                const std::string value{ avu_inp->arg4 };
                const std::string units{ NULL_PTR_GUARD(avu_inp->arg5) };

                if(config->flag == attribute) { return; }

                const std::string add{"add"};
                const std::string set{"set"};
                const std::string rm{"rm"};
                const std::string rmw{"rmw"}; // yet to be implemented; AVU args are "like" patterns to be used in a genquery
                const std::string collection{"-C"};
                const std::string data_object{"-d"};

                irods::indexing::indexer idx{_rei, config->instance_name_};
                if(operation == rm) {
                    // removed index metadata from collection
                    if(type == collection) {
                        // was the removed tag an indexing indicator
                        if(config->index == attribute) {
                            // schedule a possible purge of all indexed data in collection
                            idx.schedule_collection_operation(
                                irods::indexing::operation_type::purge,
                                logical_path,
                                _rei->rsComm->clientUser.userName,
                                value,
                                units);
                        }
                    }
                    // removed a single indexed AVU on an object or collection
                    if(type == data_object ||
                       (type == collection && config->index != attribute)) {
                        // schedule an AVU purge
                        idx.schedule_metadata_purge_event(
                                logical_path,
                                _rei->rsComm->clientUser.userName,
                                attribute,
                                value,
                                units);
                    }
                }
                else if(operation == set || operation == add) {
                    if(type == collection) {
                        // was the added tag an indexing indicator
                        if(config->index == attribute) {
                            // check the verify flag
                            if(collection_metadata_is_new) {
                                idx.schedule_collection_operation(
                                    irods::indexing::operation_type::index,
                                    logical_path,
                                    _rei->rsComm->clientUser.userName,
                                    value,
                                    units);
                            }
                        }
                    }
                    if(type == data_object ||
                       (type == collection && config->index != attribute)) {
                        idx.schedule_metadata_indexing_event(
                                logical_path,
                                _rei->rsComm->clientUser.userName,
                                attribute,
                                value,
                                units);
                    }
                }
            }
            else if("pep_api_data_obj_unlink_pre" == _rn) {
                auto it = _args.begin();
                std::advance(it, 2);
                if(_args.end() == it) {
                    THROW(
                        SYS_INVALID_INPUT_PARAM,
                        "invalid number of arguments");
                }
                const auto obj_inp = boost::any_cast<dataObjInp_t*>(*it);
                if (auto* p = getValByKey( &obj_inp->condInput, FORCE_FLAG_KW); p != 0) { rm_force_kw = p; }
                indices_for_rm_coll = get_indices_for_delete_by_query (*_rei->rsComm, obj_inp->objPath, false);
            }
            else if("pep_api_data_obj_unlink_post" == _rn) {
                auto it = _args.begin();
                std::advance(it, 2);
                if(_args.end() == it) {
                    THROW(
                        SYS_INVALID_INPUT_PARAM,
                        "invalid number of arguments");
                }
                const auto obj_inp = boost::any_cast<dataObjInp_t*>(*it);
                if ('*' != rm_force_kw[0]) { /* there was a force keyword */
                    irods::indexing::indexer idx{_rei, config->instance_name_};
                    nlohmann::json recurseInfo {{"is_collection",false}};
                    recurseInfo["indices"] = indices_for_rm_coll;
                    idx.schedule_metadata_purge_for_recursive_rm_object (obj_inp->objPath, recurseInfo);
                }
            }
            else if("pep_api_rm_coll_pre"  == _rn) {
                /**
                  *   argument spec :
                  *     <ignored>  <ignored>  <CollInp*> [...?]
                  *
                  *   before a collection is deleted. record whether FORCE_FLAG_KW is used.
                  *
                  **/
                auto it = _args.begin();
                std::advance(it, 2);
                if(_args.end() == it) {
                    THROW(
                        SYS_INVALID_INPUT_PARAM,
                        "invalid number of arguments");
                }
                CollInp*obj_inp = nullptr;
                obj_inp = boost::any_cast<CollInp*>(*it);
                if (auto* p = getValByKey( &obj_inp->condInput, FORCE_FLAG_KW); p != 0) { rm_force_kw = p; }
                indices_for_rm_coll = get_indices_for_delete_by_query( *_rei->rsComm, obj_inp->collName, true );
            }
            else if("pep_api_rm_coll_post"  == _rn) {

                /**
                  *   argument spec :
                  *     <ignored>  <ignored>  <CollInp*> [...?]
                  *
                  *   collection has been deleted successfully. If FORCE_FLAG_KW was used, then purge the
                  *   from the relevant indexes collection recursively
                  */

                if ('*' != rm_force_kw[0]) { /* there was a force keyword */
                    auto it = _args.begin();
                    std::advance(it, 2);
                    if(_args.end() == it) {
                        THROW(
                            SYS_INVALID_INPUT_PARAM,
                            "invalid number of arguments");
                    }
                    CollInp* obj_inp = nullptr;
                    obj_inp = boost::any_cast<CollInp*>(*it);
                    irods::indexing::indexer idx{_rei, config->instance_name_};
                    nlohmann::json recurseInfo = {{"is_collection",true}};
                    recurseInfo["indices"] = indices_for_rm_coll;
                    idx.schedule_metadata_purge_for_recursive_rm_object(obj_inp->collName, recurseInfo);
                }
            }
            else if (_rn == "pep_api_atomic_apply_metadata_operations_pre"
                  || _rn == "pep_api_atomic_apply_metadata_operations_post")
            {
                    using nlohmann::json;
                    auto pos = _rn.rfind("_p");
                    auto when = _rn.substr(pos + 1); // "pre" or "post"
                    auto it = _args.begin();
                    std::advance(it, 2);

                    auto request = boost::any_cast<BytesBuf*>(*it);
                    std::string requ_str {(const char*)request->buf,unsigned(request->len)};
                    const auto & requ_json = json::parse( requ_str );

                    std::string obj_path = requ_json["entity_name"]; // logical path
                    std::string obj_type = requ_json["entity_type"]; // "data_object" or "collection"
                    if ("pre" == when) {
                        atomic_metadata_obj_path = obj_path;
                    }
                    else {
                        if (atomic_metadata_obj_path != obj_path) {
                            THROW( SYS_INVALID_INPUT_PARAM, fmt::format("invalid object path for {} operation",_rn));
                        }
                    }
                    auto & map = atomic_metadata_tuples[ when ];

                    namespace fs = irods::experimental::filesystem;
                    std::string dobj_name {fs::path{obj_path}.object_name()};
                    std::string dobj_parent {fs::path{obj_path}.parent_path()};

                    auto query_str = fmt::format( "SELECT META_{0}_ATTR_NAME, META_{0}_ATTR_VALUE, META_{0}_ATTR_UNITS where {0}_NAME = '{1}'",
                                                  (obj_type == "collection" ? "COLL" : "DATA"),
                                                  (obj_type == "collection" ? obj_path : dobj_name) );
                    if (obj_type != "collection") {
                        query_str += fmt::format (" and COLL_NAME = '{}'", dobj_parent);
                    }

                    irods::query<rsComm_t> qobj {_rei->rsComm, query_str};

                    for (const auto & row : qobj) {
                        map.insert( {row[0],row[1],row[2]} );
                    }
                    if (when == "post") {

                        std::vector<metadata_tuple> avus_added_or_removed;

                        const auto & pre_map = atomic_metadata_tuples[ "pre" ];
                        set_symmetric_difference (  pre_map.begin(), pre_map.end(),
                                                    map.cbegin(), map.cend(),  std::back_inserter(avus_added_or_removed));

                        for (const auto & [attribute, value, units]: avus_added_or_removed) {
                            if (attribute != config->index) {
                                irods::indexing::indexer idx{_rei, config->instance_name_};
                                idx.schedule_metadata_indexing_event(
                                        obj_path,
                                        _rei->rsComm->clientUser.userName,
                                        attribute,
                                        value,
                                        units);
                                break;           // only need one event to re-index all AVU's
                            }
                        }
                    }
            }
        }
        catch(const boost::bad_any_cast& _e) {
            THROW(
                INVALID_ANY_CAST,
                boost::str(boost::format(
                    "function [%s] rule name [%s]")
                    % __FUNCTION__ % _rn));
        }
    } // apply_indexing_policy

    // -=-=-=-= Invoke policy on object. uses
    // -
    // - apply_object_policy (root, obj_path, src_resc, indexer, index_name, index_type)
    // -
    // - (1) Composes a policy name from (root , indexer)
    // - (2) Invokes the policy by that name upon (obj_path, src_resc, index_name, index_type)
    // -

    void apply_object_policy(
        ruleExecInfo_t*    _rei,
        const std::string& _policy_root,
        const std::string& _object_path,
        const std::string& _source_resource,
        const std::string& _indexer,
        const std::string& _index_name,
        const std::string& _index_type) {
        const std::string policy_name{irods::indexing::policy::compose_policy_name(
                              _policy_root,
                              _indexer)};

        std::list<boost::any> args;
        args.push_back(boost::any(_object_path));
        args.push_back(boost::any(_source_resource));
        args.push_back(boost::any(_index_name));
        args.push_back(boost::any(_index_type));
        irods::indexing::invoke_policy(_rei, policy_name, args);


    } // apply_object_policy

    void apply_specific_policy(
        ruleExecInfo_t*    _rei,
        const std::string& _policy_name,  // request specific policy by name
        const std::string& _object_path,
        const std::string& _source_resource,
        const std::string& _indexer,
        const std::string& _index_name,
        const std::string& _index_type) {

        std::list<boost::any> args;
        args.push_back(boost::any(_object_path));
        args.push_back(boost::any(_source_resource));
        args.push_back(boost::any(_index_name));
        args.push_back(boost::any(_index_type));
        irods::indexing::invoke_policy(_rei, _policy_name, args);

    } // apply_specific_policy

/***********/

    std::string to_lowercase (const std::string & src)
    {
        std::string dst;
        std::transform(src.begin(),src.end(),
                       std::back_inserter(dst),
                       [](wchar_t w) { return tolower(w); } );
        return dst;
    }

    auto get_default_mime_type (const std::string & input) -> std::string
    {
        const static std::map <std::string, std::string> default_mime_types
        {
            {".aac", "audio/aac"},
            {".abw", "application/x-abiword"},
            {".arc", "application/x-freearc"},
            {".avi", "video/x-msvideo"},
            {".azw", "application/vnd.amazon.ebook"},
            {".bin", "application/octet-stream"},
            {".bmp", "image/bmp"},
            {".bz", "application/x-bzip"},
            {".bz2", "application/x-bzip2"},
            {".cda", "application/x-cdf"},
            {".csh", "application/x-csh"},
            {".css", "text/css"},
            {".csv", "text/csv"},
            {".doc", "application/msword"},
            {".docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"},
            {".eot", "application/vnd.ms-fontobject"},
            {".epub", "application/epub+zip"},
            {".gz", "application/gzip"},
            {".gif", "image/gif"},
            {".htm", "text/html"},
            {".html", "text/html"},
            {".ico", "image/vnd.microsoft.icon"},
            {".ics", "text/calendar"},
            {".jar", "application/java-archive"},
            {".jpeg", "image/jpeg"},
            {".jpg", "image/jpeg"},
            {".js", "text/javascript"},
            {".json", "application/json"},
            {".jsonld", "application/ld+json"},
            {".mid", "audio/midi audio/x-midi"},
            {".midi", "audio/midi audio/x-midi"},
            {".mjs", "text/javascript"},
            {".mp3", "audio/mpeg"},
            {".mp4", "video/mp4"},
            {".mpeg", "video/mpeg"},
            {".mpkg", "application/vnd.apple.installer+xml"},
            {".odp", "application/vnd.oasis.opendocument.presentation"},
            {".ods", "application/vnd.oasis.opendocument.spreadsheet"},
            {".odt", "application/vnd.oasis.opendocument.text"},
            {".oga", "audio/ogg"},
            {".ogv", "video/ogg"},
            {".ogx", "application/ogg"},
            {".opus", "audio/opus"},
            {".otf", "font/otf"},
            {".png", "image/png"},
            {".pdf", "application/pdf"},
            {".php", "application/x-httpd-php"},
            {".ppt", "application/vnd.ms-powerpoint"},
            {".pptx", "application/vnd.openxmlformats-officedocument.presentationml.presentation"},
            {".rar", "application/vnd.rar"},
            {".rtf", "application/rtf"},
            {".sh", "application/x-sh"},
            {".svg", "image/svg+xml"},
            {".swf", "application/x-shockwave-flash"},
            {".tar", "application/x-tar"},
            {".tif", "image/tiff"},
            {".tiff", "image/tiff"},
            {".ts", "video/mp2t"},
            {".ttf", "font/ttf"},
            {".txt", "text/plain"},
            {".vsd", "application/vnd.visio"},
            {".wav", "audio/wav"},
            {".weba", "audio/webm"},
            {".webm", "video/webm"},
            {".webp", "image/webp"},
            {".woff", "font/woff"},
            {".woff2", "font/woff2"},
            {".xhtml", "application/xhtml+xml"},
            {".xls", "application/vnd.ms-excel"},
            {".xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"},
            {".xml", "application/xml"}, // if not readable from casual users (RFC 3023, section 3)" else "text/xml"
            {".xul", "application/vnd.mozilla.xul+xml"},
            {".zip", "application/zip"},
            {".3gp", "video/3gpp"}, // audio/3gpp if it doesn't contain video,
            {".3g2", "video/3gpp2"}, // audio/3gpp2 if it doesn't contain video,
            {".7z", "application/x-7z-compressed"},
        };
        std::string retvalue {};
        if (auto offs = input.rfind("."); offs != std::string::npos) {
            const std::string lower_case_ext = to_lowercase(input.substr(offs));
            try{
                retvalue = default_mime_types.at(lower_case_ext);
            }
            catch(const std::out_of_range & e) {
                irods::log(LOG_DEBUG, fmt::format("In {}, function {}: Unknown extension [{}]",__FILE__,__func__,lower_case_ext));
            }
        }
        return retvalue.size() ? retvalue : "application/octet-stream";
    }

    auto get_system_metadata( ruleExecInfo_t* _rei
                             ,const std::string& _obj_path ) -> nlohmann::json {
        using nlohmann::json;
        const boost::filesystem::path p{_obj_path};
        const std::string parent_name = p.parent_path().string();
        const std::string name = p.filename().string();
        namespace fs   = irods::experimental::filesystem;
        namespace fsvr = irods::experimental::filesystem::server;
        auto irods_path = fs::path{_obj_path};
        const auto s = fsvr::status(*_rei->rsComm, irods_path);
        std::string query_str;

        json obj;

        obj["absolutePath"] = _obj_path;

        bool is_collection = false;
        if (fsvr::is_data_object(s)) {
            query_str = fmt::format("SELECT DATA_ID , DATA_MODIFY_TIME, DATA_ZONE_NAME, COLL_NAME, DATA_SIZE where DATA_NAME = '{0}'"
                                    " and COLL_NAME = '{1}' ", name, parent_name  );
            irods::query<rsComm_t> qobj{_rei->rsComm, query_str, 1};
            for (const auto & i:qobj) {
                obj["lastModifiedDate"] = std::stol( i[1] ); // epoch seconds
                obj["zoneName"] = i[2];
                obj["parentPath"] = i[3];
                obj["dataSize"] = std::stol( i[4] );
                obj["isFile"] = true;
                break;
            }
        }
        else if (fsvr::is_collection(s)) {
            is_collection = true;
            query_str = fmt::format("SELECT COLL_ID , COLL_MODIFY_TIME, COLL_ZONE_NAME, COLL_PARENT_NAME where COLL_NAME = '{0}'"
                                    " and COLL_PARENT_NAME = '{1}' ", _obj_path, parent_name  );
            irods::query<rsComm_t> qobj{_rei->rsComm, query_str, 1};
            for (const auto & i : qobj) {
                obj["lastModifiedDate"] = std::stol( i[1] ); // epoch seconds
                obj["zoneName"] = i[2];
                obj["parentPath"] = i[3];
                obj["dataSize"] = 0L;
                obj["isFile"] = false;
                break;
            }
        }
        auto fileName = obj ["fileName"] = irods_path.object_name();
        obj ["url"] = fmt::format(fmt::runtime(config->urlTemplate), _obj_path);
        obj["mimeType"] = (is_collection ? "" : get_default_mime_type (fileName));
        return obj;

    } // get_system_metadata

    void apply_metadata_policy(
        ruleExecInfo_t*    _rei,
        const std::string& _policy_root,
        const std::string& _object_path,
        const std::string& _indexer,
        const std::string& _index_name
        // -- now used only to distinguish between object and avu purges
        ,const std::string& _attribute
        ,const std::string& _value
        ,const std::string& _units
        )
    {
        const std::string policy_name { irods::indexing::policy::compose_policy_name(
                                          _policy_root,
                                          _indexer)     };


            std::list<boost::any> args;
            args.push_back(boost::any(_object_path));
            args.push_back(boost::any(_attribute)); // was attr    //   -- not explicit anymore with new schema as AVU's are cataloged
            args.push_back(boost::any(_value)); // was value   //      within the indexed entry for the object itself
            args.push_back(boost::any(_units)); // was units   //      As this is now a no-op, we should remove these arguments.
            args.push_back(boost::any(_index_name));

            args.push_back(boost::any(get_system_metadata(_rei, _object_path).dump()));

            irods::indexing::invoke_policy(_rei, policy_name, args);

    } // apply_metadata_policy

	irods::error start(irods::default_re_ctx&, const std::string& _instance_name)
	{
		RuleExistsHelper::Instance()->registerRuleRegex("pep_api_.*");

		try {
			config = std::make_unique<irods::indexing::configuration>(_instance_name);
		}
		catch (const irods::exception& e) {
			return e;
		}

		rodsLog(LOG_DEBUG, "value of minimum_delay_time: %d", config->minimum_delay_time);
		rodsLog(LOG_DEBUG, "value of maximum_delay_time: %d", config->maximum_delay_time);
		rodsLog(LOG_DEBUG, "value of job_limit_per_collection_indexing_operation: %d", config->job_limit);

		return SUCCESS();
	} // start

	irods::error stop(
		irods::default_re_ctx&,
		const std::string& ) {
		return SUCCESS();
	} // stop

	irods::error rule_exists(
		irods::default_re_ctx&,
		const std::string& _rn,
		bool&              _ret) {
		const std::set<std::string> rules{
										"pep_api_atomic_apply_metadata_operations_pre",
										"pep_api_atomic_apply_metadata_operations_post",
										"pep_api_data_obj_open_post",
										"pep_api_data_obj_create_post",
										"pep_api_data_obj_repl_post",
										"pep_api_data_obj_unlink_pre",
										"pep_api_data_obj_unlink_post",
										"pep_api_mod_avu_metadata_pre",
										"pep_api_mod_avu_metadata_post",
										"pep_api_data_obj_close_post",
										"pep_api_data_obj_put_post",
										"pep_api_phy_path_reg_post",
										"pep_api_rm_coll_pre",
										"pep_api_rm_coll_post",
		};
		_ret = rules.find(_rn) != rules.end();

		return SUCCESS();
	} // rule_exists

	irods::error list_rules(irods::default_re_ctx&, std::vector<std::string>&) {
		return SUCCESS();
	} // list_rules

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
			apply_indexing_policy(_rn, rei, _args);
		}
		catch(const  std::invalid_argument& _e) {
			irods::indexing::exception_to_rerror(
				SYS_NOT_SUPPORTED,
				_e.what(),
				rei->rsComm->rError);
			return ERROR(
					   SYS_NOT_SUPPORTED,
					   _e.what());
		}
		catch(const std::domain_error& _e) {
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

		return CODE(RULE_ENGINE_CONTINUE);

	} // exec_rule

	irods::error exec_rule_text(
		irods::default_re_ctx&,
		const std::string&  _rule_text,
		msParamArray_t*     _ms_params,
		const std::string&  _out_desc,
		irods::callback     _eff_hdlr) {
		using json = nlohmann::json;

		try {
			// skip the first line: @external
			std::string rule_text{_rule_text};
			if(_rule_text.find("@external") != std::string::npos) {
				rule_text = _rule_text.substr(10);
			}
			const auto rule_obj = json::parse(rule_text);
			const std::string& rule_engine_instance_name = rule_obj["rule-engine-instance-name"];
			// if the rule text does not have our instance name, fail
			if(config->instance_name_ != rule_engine_instance_name) {
				return ERROR(
						SYS_NOT_SUPPORTED,
						"instance name not found");
			}
#if 0
			// catalog / index drift correction
			if(irods::indexing::schedule::indexing ==
			   rule_obj["rule-engine-operation"]) {
				ruleExecInfo_t* rei{};
				const auto err = _eff_hdlr("unsafe_ms_ctx", &rei);
				if(!err.ok()) {
					return err;
				}

				const std::string& params = rule_obj["delay-parameters"];

				json delay_obj;
				delay_obj["rule-engine-operation"] = irods::indexing::policy::indexing;

				irods::indexing::indexer idx{rei, config->instance_name_};
				idx.schedule_indexing_policy(
					delay_obj.dump(),
					params);
			}
			else
#endif
			{
				return ERROR(
						SYS_NOT_SUPPORTED,
						"supported rule name not found");
			}
		}
		catch(const  std::invalid_argument& _e) {
			std::string msg{"Rule text is not valid JSON -- "};
			msg += _e.what();
			return ERROR(
					   SYS_NOT_SUPPORTED,
					   msg);
		}
		catch(const std::domain_error& _e) {
			std::string msg{"Rule text is not valid JSON -- "};
			msg += _e.what();
			return ERROR(
					   SYS_NOT_SUPPORTED,
					   msg);
		}
		catch(const irods::exception& _e) {
			return ERROR(
					_e.code(),
					_e.what());
		}

		return SUCCESS();
	} // exec_rule_text

	irods::error exec_rule_expression(
		irods::default_re_ctx&,
		const std::string&     _rule_text,
		msParamArray_t*        _ms_params,
		irods::callback        _eff_hdlr) {
		using json = nlohmann::json;
		ruleExecInfo_t* rei{};
		const auto err = _eff_hdlr("unsafe_ms_ctx", &rei);
		if(!err.ok()) {
			return err;
		}

		try {
			const auto rule_obj = json::parse(_rule_text);
			if(irods::indexing::policy::object::index ==
			   rule_obj["rule-engine-operation"]) {
				try {
					// proxy for provided user name
					const std::string& user_name = rule_obj["user-name"];
					rstrcpy(
						rei->rsComm->clientUser.userName,
						user_name.c_str(),
						NAME_LEN);

					// - implement (full-text?) indexing on an individual object
					// -     as a delayed task.
					// -
					apply_object_policy(
						rei,
						irods::indexing::policy::object::index,
						rule_obj["object-path"],
						rule_obj["source-resource"],
						rule_obj["indexer"],
						rule_obj["index-name"],
						rule_obj["index-type"]);
				}
				catch(const irods::exception& _e) {
					printErrorStack(&rei->rsComm->rError);
					return ERROR(
							_e.code(),
							_e.what());
				}
			}
			else if(irods::indexing::policy::object::purge ==
					rule_obj["rule-engine-operation"]) {
				try {
					// proxy for provided user name
					const std::string& user_name = rule_obj["user-name"];
					rstrcpy(
						rei->rsComm->clientUser.userName,
						user_name.c_str(),
						NAME_LEN);

					// - implement index purge on an individual object
					// -    as a delayed task.
					// -
					apply_object_policy(
						rei,
						irods::indexing::policy::object::purge,
						rule_obj["object-path"],
						rule_obj["source-resource"],
						rule_obj["indexer"],
						rule_obj["index-name"],
						rule_obj["index-type"]);
				}
				catch(const irods::exception& _e) {
					printErrorStack(&rei->rsComm->rError);
					return ERROR(
							_e.code(),
							_e.what());
				}
			}
			else if(irods::indexing::policy::collection::index ==
					rule_obj["rule-engine-operation"]) {

				// - launch delayed task to handle indexing events under a collection
				// -   ( example : a new indexing AVU was placed on the collection )
				// -
				irods::indexing::indexer idx{rei, config->instance_name_};
				idx.schedule_policy_events_for_collection(
					irods::indexing::operation_type::index,
					rule_obj["collection-name"],
					rule_obj["user-name"],
					rule_obj["indexer"],
					rule_obj["index-name"],
					rule_obj["index-type"]);
			}
			else if(irods::indexing::policy::collection::purge ==
					rule_obj["rule-engine-operation"]) {

				// - launch delayed task to handle indexing events under a collection
				// -   ( example : an indexing AVU was removed from the collection )
				// -
				irods::indexing::indexer idx{rei, config->instance_name_};
				idx.schedule_policy_events_for_collection(
					irods::indexing::operation_type::purge,
					rule_obj["collection-name"],
					rule_obj["user-name"],
					rule_obj["indexer"],
					rule_obj["index-name"],
					rule_obj["index-type"]);
			}
			else if(irods::indexing::policy::metadata::index ==
					rule_obj["rule-engine-operation"]) {
				try {
					// proxy for provided user name
					const std::string& user_name = rule_obj["user-name"];
					rstrcpy(
						rei->rsComm->clientUser.userName,
						user_name.c_str(),
						NAME_LEN);

					apply_metadata_policy(
						rei,
						irods::indexing::policy::metadata::index,
						rule_obj["object-path"],
						rule_obj["indexer"],
						rule_obj["index-name"]
						 , rule_obj["attribute"]
						 , rule_obj["value"]
						 , rule_obj["units"]
					);
				}
				catch(const irods::exception& _e) {
					printErrorStack(&rei->rsComm->rError);
					return ERROR(
							_e.code(),
							_e.what());
				}
			}
			else if(irods::indexing::policy::metadata::purge ==
					rule_obj["rule-engine-operation"]) {
				try {
					// proxy for provided user name
					const std::string& user_name = rule_obj["user-name"];
					rstrcpy(
						rei->rsComm->clientUser.userName,
						user_name.c_str(),
						NAME_LEN);

					apply_metadata_policy(
						rei,
						irods::indexing::policy::metadata::purge,
						rule_obj["object-path"],
						rule_obj["indexer"],
						rule_obj["index-name"]
						, rule_obj["attribute"]
						, rule_obj["value"]
						, rule_obj["units"]
						);
				}
				catch(const irods::exception& _e) {
					printErrorStack(&rei->rsComm->rError);
					return ERROR(
							_e.code(),
							_e.what());
				}
			}
			else if("irods_policy_recursive_rm_object_by_path" == rule_obj["rule-engine-operation"]) {

					const std::string& user_name = rule_obj["user-name"];
					rstrcpy(
						rei->rsComm->clientUser.userName,
						user_name.c_str(),
						NAME_LEN);

					apply_specific_policy(
						rei,
						"irods_policy_recursive_rm_object_by_path",
						rule_obj["object-path"],
						rule_obj["source-resource"],
						rule_obj["indexer"],
						rule_obj["index-name"],
						rule_obj["index-type"]);
			}
			else {
				printErrorStack(&rei->rsComm->rError);
				return ERROR(
						SYS_NOT_SUPPORTED,
						"supported rule name not found");
			}
		}
		catch(const  json::parse_error& _e) {
			rodsLog(LOG_ERROR,"Exception (%s). Could not parse JSON rule text @ FILE %s LINE %d FUNCTION %s ",
								_e.what(),__FILE__,__LINE__,__FUNCTION__);
			return CODE( RULE_ENGINE_CONTINUE);
		}
		catch(const  std::invalid_argument& _e) {
			return ERROR(
					   SYS_NOT_SUPPORTED,
					   _e.what());
		}
		catch(const std::domain_error& _e) {
			return ERROR(
					   SYS_NOT_SUPPORTED,
					   _e.what());
		}
		catch(const irods::exception& _e) {
			return ERROR(
					_e.code(),
					_e.what());
		}

		return SUCCESS();

	} // exec_rule_expression
} // namespace

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
