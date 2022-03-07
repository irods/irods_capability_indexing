#ifndef CONFIGURATION_HPP
#define CONFIGURATION_HPP

#include <string>
#include <irods/rodsLog.h>
#include <irods/irods_log.hpp>

namespace irods {
    namespace indexing {

        namespace policy {
            // Policy Naming Examples
            // irods_policy_<namespace>_<subject>_<operation>_<technology>
            // irods_policy_indexing_object_index_
            // irods_policy_indexing_collection_index_
            // irods_policy_indexing_metadata_index_
            // irods_policy_indexing_object_purge_
            // irods_policy_indexing_collection_purge_
            // irods_policy_indexing_metadata_purge_

            static constexpr auto prefix = "irods_policy_indexing";
            std::string compose_policy_name(
                    const std::string& _prefix,
                    const std::string& _technology);


            namespace object {
                static const std::string index{"irods_policy_indexing_object_index"};
                static const std::string purge{"irods_policy_indexing_object_purge"};
            } // object

            namespace metadata {
                static const std::string index{"irods_policy_indexing_metadata_index"};
                static const std::string purge{"irods_policy_indexing_metadata_purge"};
            } // metadata

            namespace collection {
                static const std::string index{"irods_policy_indexing_collection_index"};
                static const std::string purge{"irods_policy_indexing_collection_purge"};
            } // collection

        } // policy

        std::string operation_and_index_types_to_policy_name(
                const std::string& _operation_type,
                const std::string& _index_type);

        namespace schedule {
            static const std::string object{"irods_policy_schedule_object_indexing"};
            static const std::string collection{"irods_policy_schedule_collection_indexing"};
        }

        namespace index_type {
            static const std::string full_text{"full_text"};
            static const std::string metadata{"metadata"};
        }

        namespace operation_type {
            static const std::string index{"index"};
            static const std::string purge{"purge"};
        }

        struct configuration {
            // metadata attributes
            std::string index{"irods::indexing::index"};
            std::string flag{"irods::indexing::flag"};

            // basic configuration
            std::string minimum_delay_time{"1"};
            std::string maximum_delay_time{"30"};
            std::string job_limit{""};
            std::string delay_parameters{"<EF>60s DOUBLE UNTIL SUCCESS OR 5 TIMES</EF>"};
            std::string urlTemplate{"http:/{}"};  // Clients should aim not to use this index-embedded URL. It is no longer used by
                                                  // MetaLnx as of version 2.5.0, nor by the search plugin extension.
                                                  // Thus, it is a possible target for deprecation.
            int log_level{LOG_DEBUG};
            std::string collection_test_flag {""};

            const std::string instance_name_{};
            explicit configuration(const std::string& _instance_name);
        }; // struct configuration
    } // namespace indexing
} // namespace irods

#endif // STORAGE_TIERING_CONFIGURATION_HPP
