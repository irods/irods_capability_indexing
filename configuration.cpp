
#include "configuration.hpp"
#include "plugin_specific_configuration.hpp"

namespace irods {
    namespace indexing {
        configuration::configuration(
            const std::string& _instance_name ) :
            instance_name_{_instance_name} {
            try {
                auto cfg = get_plugin_specific_configuration(_instance_name);
                auto capture_parameter = [&](const std::string& _param, std::string& _attr) {
                    if(cfg.find(_param) != cfg.end()) {
                        _attr = boost::any_cast<std::string>(cfg.at(_param));
                    }
                }; // capture_parameter

                // integer-or-string parameters

                using configuration_parameters::load;

                job_limit = load<int>(cfg, "job_limit_per_collection_indexing_operation");
                minimum_delay_time = load<int>(cfg, "minimum_delay_time", 1);
                maximum_delay_time = load<int>(cfg, "maximum_delay_time", 30);

                // string parameters

                capture_parameter("index", index);
                capture_parameter("url_template", urlTemplate);
                capture_parameter("delay_parameters", delay_parameters);
                capture_parameter("collection_test_flag", collection_test_flag);

            } catch ( const boost::bad_any_cast& _e ) {
                THROW( INVALID_ANY_CAST, _e.what() );
            } catch ( const exception _e ) {
                THROW( KEY_NOT_FOUND, _e.what() );
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

