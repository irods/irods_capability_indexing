#ifndef PLUGIN_SPECIFIC_CONFIGURATION_HPP
#define PLUGIN_SPECIFIC_CONFIGURATION_HPP
#include <string>
#include "boost/any.hpp"
#include <unordered_map>
#include "irods_exception.hpp"
#include "rodsErrorTable.h"

namespace irods {
    namespace indexing {
        using plugin_specific_configuration = std::unordered_map<std::string, boost::any>;
        plugin_specific_configuration get_plugin_specific_configuration(const std::string& _instance_name);
    } // namespace indexing
} // namespace irods
#endif // PLUGIN_SPECIFIC_CONFIGURATION_HPP
