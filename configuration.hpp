#ifndef IRODS_CAPABILITY_INDEXING_CONFIGURATION_HPP
#define IRODS_CAPABILITY_INDEXING_CONFIGURATION_HPP

#include <irods/irods_log.hpp>
#include <irods/rodsLog.h>

#include <boost/any.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/optional.hpp>

#include <fmt/format.h>
#include <nlohmann/json.hpp>

#include <string>
#include <unordered_map>

namespace irods::indexing
{
	namespace configuration_parameters
	{
		const auto VERBOSE_CONFIGURATION_LOADING = false;

		// Specializations of map_traits handle loading of configuration from:
		//
		//    * unordered_map<string,boost::any> (iRODS <= 4.2)
		//    * nlohmann::json                   (iRODS >= 4.3)

		template <typename Map>
		struct map_traits
		{
		};

		template <>
		struct map_traits<nlohmann::json>
		{
			using value_type = nlohmann::json;
			using key_absent = nlohmann::json::out_of_range;
		};

		template <typename V>
		struct map_traits<std::unordered_map<std::string, V>>
		{
			using value_type = V;
			using key_absent = std::out_of_range;
		};

		// For internal handling of absent keys.

		struct no_such_key : std::runtime_error
		{
			explicit no_such_key(const std::string& key)
				: std::runtime_error{std::string{"No such key: "} + key}
			{
			}
		};

		// Load a value from a configuration map by key name.

		template <typename M>
		auto value_slot_by_name(const M& m, const std::string& name) -> const typename map_traits<M>::value_type&
		{
			try {
				return m.at(name);
			}
			catch (const typename map_traits<M>::key_absent&) {
				throw no_such_key{name};
			}
		}

		// Helper class to get a value of the expected type T.
		// T will most likely be a std::string, int, double, or bool.

		template <typename T>
		class getter
		{
		  public:
			using json = nlohmann::json;

		  private:
			// Helper methods for the two map types we support:

			template <typename U = T>
			bool impl_(const json& j, boost::optional<U>& u)
			{
				try {
					u = j.get<U>();
				}
				catch (const json::type_error&) {
					return {};
				}
				return true;
			}

			template <typename U = T>
			bool impl_(const boost::any& a, boost::optional<U>& u)
			{
				try {
					u = boost::any_cast<U>(a);
				}
				catch (const boost::bad_any_cast&) {
					return {};
				}
				return true;
			}

		  public:
			// The interface: 'get'
			//   - tries to retrieve a stored value:  First by the expected type T,
			//     or (failing that) from a string in the config.
			//   - returns true if the holder value contains a value successfully
			//     so loaded from the configuration.

			template <typename M>
			bool get(const M& m, boost::optional<T>& t)
			{
				if (impl_(m, t)) {
					return true;
				}
				boost::optional<std::string> text;
				if (impl_(m, text)) {
					t = boost::lexical_cast<T>(*text);
					return true;
				}
				return false;
			}
		};

		// High level configuration value loader:
		// =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
		// Inputs:
		//    * map : the iRODS 4.2/4.3 map object or subobject capable of being indexed by string
		//    * key : the name of the configuration variable to be loaded
		//    * (optional) default_v : the value to be returned if the value cannot be found or translated properly
		//    *                        (If omitted, the default constructed value for the type T is used.)
		//
		// Returns: the desired configuration variable of the type T

		template <typename T, typename Map>
		const T load(const Map& map, const char* key, const T& default_v = T{})
		{
			using namespace std;
			boost::optional<T> retval;
			std::string error_msg = "";
			try {
				auto& v = value_slot_by_name<Map>(map, key);
				if (!getter<T>{}.get(v, retval)) {
					if (VERBOSE_CONFIGURATION_LOADING) {
						irods::log(LOG_ERROR, fmt::format("type or format error loading key {}", key));
					}
				}
			}
			catch (const no_such_key& e) { // key not found; fall through to rely on the default value
			}
			catch (std::exception& e) {
				error_msg = fmt::format("Exception - {}", e.what());
			}
			catch (...) {
				error_msg = "Unknown error";
			}

			if (error_msg.size()) {
				irods::log(LOG_ERROR, error_msg);
			}
			if (!retval) {
				if (VERBOSE_CONFIGURATION_LOADING) {
					irods::log(LOG_ERROR, fmt::format("Using default value of {} = {}", key, default_v));
				}
				return default_v;
			}
			return *retval;
		}

	} //namespace configuration_parameters

	namespace policy
	{
		// Policy Naming Examples
		// irods_policy_<namespace>_<subject>_<operation>_<technology>
		// irods_policy_indexing_object_index_
		// irods_policy_indexing_collection_index_
		// irods_policy_indexing_metadata_index_
		// irods_policy_indexing_object_purge_
		// irods_policy_indexing_collection_purge_
		// irods_policy_indexing_metadata_purge_

		static constexpr auto prefix = "irods_policy_indexing";
		std::string compose_policy_name(const std::string& _prefix, const std::string& _technology);

		namespace object
		{
			static const std::string index{"irods_policy_indexing_object_index"};
			static const std::string purge{"irods_policy_indexing_object_purge"};
		} // namespace object

		namespace metadata
		{
			static const std::string index{"irods_policy_indexing_metadata_index"};
			static const std::string purge{"irods_policy_indexing_metadata_purge"};
		} // namespace metadata

		namespace collection
		{
			static const std::string index{"irods_policy_indexing_collection_index"};
			static const std::string purge{"irods_policy_indexing_collection_purge"};
		} // namespace collection
	}     // namespace policy

	std::string operation_and_index_types_to_policy_name(const std::string& _operation_type,
	                                                     const std::string& _index_type);

	namespace schedule
	{
		static const std::string object{"irods_policy_schedule_object_indexing"};
		static const std::string collection{"irods_policy_schedule_collection_indexing"};
	} // namespace schedule

	namespace index_type
	{
		static const std::string full_text{"full_text"};
		static const std::string metadata{"metadata"};
	} // namespace index_type

	namespace operation_type
	{
		static const std::string index{"index"};
		static const std::string purge{"purge"};
	} // namespace operation_type

	struct configuration
	{
		// metadata attributes
		std::string index{"irods::indexing::index"};
		std::string flag{"irods::indexing::flag"};

		// basic configuration
		int minimum_delay_time{1};
		int maximum_delay_time{30};
		int job_limit{};
		std::string delay_parameters{"<EF>60s DOUBLE UNTIL SUCCESS OR 5 TIMES</EF>"};

		int log_level{LOG_DEBUG};
		std::string collection_test_flag;

		const std::string instance_name;

		explicit configuration(const std::string& _instance_name);
	}; // struct configuration
} // namespace irods::indexing

#endif // IRODS_CAPABILITY_INDEXING_CONFIGURATION_HPP
