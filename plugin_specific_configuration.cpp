#include "plugin_specific_configuration.hpp"

#include <irods/irods_exception.hpp>
#include <irods/irods_server_properties.hpp>
#include <irods/rodsLog.h>

#include <fmt/format.h>

namespace irods::indexing
{
	plugin_specific_configuration get_plugin_specific_configuration(const std::string& _instance_name)
	{
		try {
			const auto& rule_engines = get_server_property<const nlohmann::json&>(
				std::vector<std::string>{KW_CFG_PLUGIN_CONFIGURATION, KW_CFG_PLUGIN_TYPE_RULE_ENGINE});
			for (const auto& rule_engine : rule_engines) {
				const auto& inst_name = rule_engine.at(KW_CFG_INSTANCE_NAME).get_ref<const std::string&>();
				if (inst_name == _instance_name) {
					if (rule_engine.count(KW_CFG_PLUGIN_SPECIFIC_CONFIGURATION) > 0) {
						return rule_engine.at(KW_CFG_PLUGIN_SPECIFIC_CONFIGURATION);
					}
				}
			}
		}
		catch (const std::out_of_range& e) {
			THROW(KEY_NOT_FOUND, e.what());
		}
		catch (const nlohmann::json::exception& _e) {
			rodsLog(LOG_ERROR,
			        fmt::format("[{}:{}] in [file={}] - json exception occurred [error={}], [instance={}]",
			                    __func__,
			                    __LINE__,
			                    __FILE__,
			                    _e.what(),
			                    _instance_name)
			            .c_str());
			THROW(SYS_LIBRARY_ERROR, _e.what());
		}
		catch (const std::exception& e) {
			rodsLog(LOG_ERROR, "General exception in %s - %s", __func__, e.what());
			THROW(SYS_INTERNAL_ERR, e.what());
		}
		catch (...) {
			THROW(SYS_UNKNOWN_ERROR, fmt::format("Function {} File {} Line {}", __func__, __FILE__, __LINE__));
		}

		THROW(SYS_INVALID_INPUT_PARAM,
		      fmt::format("failed to find configuration for indexing plugin [{}]", _instance_name));
	} // get_plugin_specific_configuration
} // namespace irods::indexing
