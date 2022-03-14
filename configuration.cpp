
#include "configuration.hpp"
#include "plugin_specific_configuration.hpp"
#include <fmt/format.h>
#include <irods/rodsLog.h>
#include <irods/irods_log.hpp>

namespace irods {
    namespace indexing {
        configuration::configuration(
            const std::string& _instance_name ) :
            instance_name_{_instance_name} {
            try {
                auto cfg = get_plugin_specific_configuration(_instance_name);
                auto capture_parameter = [&](const std::string& _param, std::string& _attr) {
                    if (const auto iter = cfg.find(_param); iter != cfg.end()) {
                        _attr = iter->get<std::string>();
                    }
                }; // capture_parameter

                capture_parameter("index", index);
                capture_parameter("minimum_delay_time", minimum_delay_time);
                capture_parameter("job_limit_per_collection_indexing_operation", job_limit);
                capture_parameter("url_template", urlTemplate);
                capture_parameter("maximum_delay_time", maximum_delay_time);
                capture_parameter("delay_parameters",   delay_parameters);
                capture_parameter("collection_test_flag",  collection_test_flag);
            } catch ( const exception& _e ) {
                THROW( KEY_NOT_FOUND, fmt::format("[{}:{}] - [{}] [error_code=[{}], instance_name=[{}]",
                                      __func__, __LINE__, _e.client_display_what(), _e.code(), _instance_name));
            } catch ( const nlohmann::json::exception& _e ) {
                irods::log( LOG_ERROR,
                            fmt::format("[{}:{}] in [file={}] - json exception occurred [error={}], [instance_name={}]",
                                         __func__,__LINE__,__FILE__, _e.what(), _instance_name));
                THROW( SYS_LIBRARY_ERROR, _e.what() );
            } catch ( const std::exception& _e ) {
                THROW( SYS_INTERNAL_ERR,
                            fmt::format("[{}:{}] in [file={}] - general exception occurred [error={}], [instance_name={}]",
                                         __func__,__LINE__,__FILE__, _e.what(), _instance_name));
            } catch ( ... ) {
                THROW( SYS_UNKNOWN_ERROR,
                       fmt::format( "[{}:{}] in [file={}], [instance_name={}]",__func__,__LINE__,__FILE__,_instance_name));
            }

        } // ctor configuration

        namespace policy {
            std::string compose_policy_name(
                    const std::string& _prefix,
                    const std::string& _technology) {
                return _prefix+"_"+_technology;
            }
        }

        std::string operation_and_index_types_to_policy_name(
                const std::string& _operation_type,
                const std::string& _index_type) {
            if(operation_type::index == _operation_type) {
                if(index_type::full_text == _index_type) {
                    return policy::object::index;
                }
                else if(index_type::metadata == _index_type) {
                    return policy::metadata::index;
                }
            }
            else if(operation_type::purge == _operation_type) {
                if(index_type::full_text == _index_type) {
                    return policy::object::purge;
                }
                else if(index_type::metadata == _index_type) {
                    return policy::metadata::purge;
                }
            } // else

            THROW(
                SYS_INVALID_INPUT_PARAM,
                boost::format("operation [%s], index [%s]")
                % _operation_type
                % _index_type);
        } // operation_and_index_types_to_policy_name
    } // namespace indexing
} // namepsace irods

