#ifndef INDEXING_UTILITIES_HPP
#define INDEXING_UTILITIES_HPP

#include "configuration.hpp"

#include <irods/irods_re_structs.hpp>
#include <irods/rcMisc.h>

#include <boost/any.hpp>
#include <nlohmann/json.hpp>

#include <string>

namespace irods {
    namespace indexing {
        class indexer {

            public:
            indexer(
                ruleExecInfo_t*    _rei,
                const std::string& _instance_name);

            void schedule_metadata_purge_for_recursive_rm_object( const std::string& logical_path,
                                                                  const nlohmann::json & recurse_info);

            void schedule_indexing_policy(
                const std::string& _json,
                const std::string& _params);

            bool metadata_exists_on_collection(
                const std::string& _collection_name,
                const std::string& _attribute,
                const std::string& _value,
                const std::string& _units );

            void schedule_collection_operation(
                const std::string& _operation,
                const std::string& _object_path,
                const std::string& _user_name,
                const std::string& _indexer_string,
                const std::string& _indexer);

            void schedule_policy_events_for_collection(
                const std::string& _policy_name,
                const std::string& _object_path,
                const std::string& _user_name,
                const std::string& _indexer,
                const std::string& _indexer_name,
                const std::string& _indexer_type);

            void schedule_full_text_indexing_event(
                const std::string& _object_path,
                const std::string& _user_name,
                const std::string& _source_resource);

            void schedule_full_text_purge_event(
                const std::string& _object_path,
                const std::string& _user_name);

            void schedule_metadata_indexing_event(
                const std::string& _object_path,
                const std::string& _user_name,
                const std::string& _attribute = {},
                const std::string& _value = {},
                const std::string& _units = {});

            void schedule_metadata_purge_event(
                const std::string& _object_path,
                const std::string& _user_name,
                const std::string& _attribute = {},
                const std::string& _value = {},
                const std::string& _units = {},
                const std::string& opt_id = {});

            private:
            std::string generate_delay_execution_parameters();

            void schedule_policy_events_given_object_path(
                const std::string& _operation_type,
                const std::string& _index_type,
                const std::string& _object_path,
                const std::string& _user_name,
                const std::string& _source_resource = {},
                const std::string& _attribute = {},
                const std::string& _value = {},
                const std::string& _units = {},
                const std::string& opt_id = {});

            void get_metadata_for_data_object(
                const std::string& _meta_attr_name,
                const std::string& _object_path,
                std::string&       _value,
                std::string&       _unit );

            using metadata_results = std::vector<std::pair<std::string, std::string>>;
            metadata_results
            get_metadata_for_collection(
                const std::string& _meta_attr_name,
                const std::string& _collection_name);

            void schedule_policy_event_for_object(
                const std::string& _event,
                const std::string& _object_path,
                const std::string& _user_name,
                const std::string& _source_resource,
                const std::string& _indexer,
                const std::string& _index_name,
                const std::string& _index_type,
                const std::string& _data_movement_params,
                const std::string& _attribute = {},
                const std::string& _value = {},
                const std::string& _units = {},
                const nlohmann::json & _extra_options = {}
            );

            std::vector<std::string> get_indexing_resource_names();

            std::string get_indexing_resource_name_for_object(
                const std::string        _object_path,
                std::vector<std::string> _resource_names);

            bool resource_is_indexable(
                const std::string        _source_resource,
                std::vector<std::string> _resource_names);

            // Attributes
            ruleExecInfo_t*rei_;
            rsComm_t*      comm_;
            configuration   config_;

            const std::string EMPTY_RESOURCE_NAME{"EMPTY_RESOURCE_NAME"};

            public: const configuration& get_config() { return config_; }
        }; // class indexer
    } // namespace indexing
} // namespace irods

#endif // INDEXING_UTILITIES_HPP

