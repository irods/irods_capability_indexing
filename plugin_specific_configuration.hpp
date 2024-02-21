#ifndef IRODS_CAPABILITY_INDEXING_PLUGIN_SPECIFIC_CONFIGURATION_HPP
#define IRODS_CAPABILITY_INDEXING_PLUGIN_SPECIFIC_CONFIGURATION_HPP

#include <irods/irods_exception.hpp>
#include <irods/rodsErrorTable.h>

#include <nlohmann/json.hpp>

#include <string>

namespace irods::indexing
{
	using plugin_specific_configuration = nlohmann::json;

	plugin_specific_configuration get_plugin_specific_configuration(const std::string& _instance_name);
} // namespace irods::indexing

#endif // IRODS_CAPABILITY_INDEXING_PLUGIN_SPECIFIC_CONFIGURATION_HPP
