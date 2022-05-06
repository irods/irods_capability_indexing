#ifndef IRODS_INDEXING_PATH_CALC_HPP
#define IRODS_INDEXING_PATH_CALC_HPP

#include <set>
#include <map>
#include <string>
#include <functional>

namespace irods {
    namespace indexing {

        //  Thrown by class path_calc_and_cache, when given a path
        //  that is not well-formed. This will result from any attempt
        //  to ascend a collection hierarchy not starting with "/".

        class path_format_error: public std::runtime_error {
        public:
            path_format_error(const std::string & s) : std::runtime_error(s.c_str()) {}
        };

        //  Thrown by class path_calc_and_cache, when it encounters an internal error caused e.g. by
        //  inserting into a std::map. (This should never happen).

        class path_property_caching_error: public std::runtime_error {
        public:
            path_property_caching_error(const std::string & s) : std::runtime_error(s.c_str()) {}
        };

        //  This class lets us compute and store the cumulative set of items, of the given Property type,
        //  that are applicable at a given level of the iRODS collection hierarchy.

        template <typename Property>
        class path_calc_and_cache {

        public:
            using properties = std::set<Property>;

        private:
            std::map<std::string, properties> path_properties;
            using calc_function = std::function<properties (const std::string& s)> ;
            calc_function calc_;

        public:

            //  Initialize with a callable object that returns the Property value for a given collection path
            //  within the hierarchy.

            explicit path_calc_and_cache(calc_function f)
                : calc_{f}
            {
            }

            //  Get properties contribution for parents (recursive up to '/')
            //  and merge set members for the current path.

            properties accum(const std::string &path)
            {
                using namespace std::string_literals;
                auto pos = (path.size() < 1 ? std::string::npos : path.find_last_of("/"));
                if (pos == std::string::npos) {
                    throw path_format_error{ "Couldn't parse: "s + path };
                }
                if (path == "/") {
                    return calc("/");
                }
                auto parent_path_length = (pos == 0) ? 1 : pos;
                auto accumulated_set = accum(path.substr(0, parent_path_length));
                const auto& current_path_set = calc(path);
                accumulated_set.insert(current_path_set.begin(), current_path_set.end());
                return accumulated_set;
            }

            //  Get individual contribution for last elem of path 'p'.
            //  Use cached value if it exists, else do the calculation

            const properties& calc(const std::string & path) {
                using namespace std::string_literals;
                auto it = path_properties.find(path);
                if (std::end(path_properties) != it) {
                    return it->second;
                }
                auto result = path_properties.insert( {path, (this->calc_)(path)} );
                if (!result.second) {
                    throw path_property_caching_error{ "Couldn't cache properties for path: "s + path };
                }
                return (*result.first).second;
            }

        }; // class path_calc_and_cache

    } // namespace indexing

} // namespace irods

#endif // IRODS_INDEXING_PATH_CALC_HPP
