
#include "plugin_specific_configuration.hpp"
#include "irods_server_properties.hpp"
#include "irods_exception.hpp"
#include "fmt/format.h"
#include "rodsLog.h"


namespace irods {
    namespace indexing {
         plugin_specific_configuration get_plugin_specific_configuration(
            const std::string& _instance_name ) {
            try {
                const auto& rule_engines = get_server_property<
                    const nlohmann::json&>(
                            std::vector<std::string>{
                            CFG_PLUGIN_CONFIGURATION_KW,
                            PLUGIN_TYPE_RULE_ENGINE});
                for ( const auto& rule_engine : rule_engines ) {
                    const auto& inst_name = rule_engine.at( CFG_INSTANCE_NAME_KW ).get_ref<const std::string&>();
                    if ( inst_name == _instance_name ) {
                        if(rule_engine.count(CFG_PLUGIN_SPECIFIC_CONFIGURATION_KW) > 0) {
                            return rule_engine.at(CFG_PLUGIN_SPECIFIC_CONFIGURATION_KW);
                        } // if has PSC
                    } // if inst_name
                } // for rule_engines

            } catch ( const std::out_of_range& e ) {
                THROW( KEY_NOT_FOUND, e.what() );

            } catch ( const nlohmann::json::exception& _e ) {
                rodsLog(LOG_ERROR, "JSON error -- Function %s  Line %d",__func__,__LINE__);
                THROW( SYS_LIBRARY_ERROR, _e.what() );

            } catch ( const std::exception& e ) {
                rodsLog(LOG_ERROR, "General exception in %s - %s", __func__, e.what());
                THROW(SYS_INTERNAL_ERR, e.what());

            } catch ( ... ) {
                THROW( SYS_UNKNOWN_ERROR, fmt::format( "Function {} File {} Line {}",__func__,__FILE__,__LINE__));
            }

            THROW(
                SYS_INVALID_INPUT_PARAM,
                boost::format("failed to find configuration for indexing plugin [%s]") %
                _instance_name);
        } // get_plugin_specific_configuration


    } // namespace indexing
} // namespace irods
