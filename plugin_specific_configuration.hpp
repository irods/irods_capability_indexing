#ifndef PLUGIN_SPECIFIC_CONFIGURATION_HPP
#define PLUGIN_SPECIFIC_CONFIGURATION_HPP
#include <string>
#include <irods/irods_exception.hpp>
#include <irods/rodsErrorTable.h>
#include <nlohmann/json.hpp>

namespace irods {
    namespace indexing {
        using plugin_specific_configuration = nlohmann::json;
        plugin_specific_configuration get_plugin_specific_configuration(const std::string& _instance_name);
    } // namespace indexing
} // namespace irods
#endif // PLUGIN_SPECIFIC_CONFIGURATION_HPP
