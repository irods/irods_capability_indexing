#ifndef CPP_JSON_KW__HPP
#define CPP_JSON_KW__HPP

#include <irods/rodsLog.h>

#include <nlohmann/json.hpp>

#include <optional>
#include <stdexcept>
#include <string>

using nlohmann::json;

template <typename T>
struct mapped_json_value {bool success ; std::optional<T> value;};

// Extract a value by T (the type) and key (a string lookup key from a JSON object)
// The returned values are, in order:
//   -  a boolean (success flag) 
//   -  a std::optional containing the retrieved value on success.

template <typename T>
auto kws_get(const nlohmann::json &j, const std::string & key) -> mapped_json_value<T>
{
   if (auto iter = j.find(key); iter != j.end()) {
       try {
           return { true, iter->get<T>() };
       }
       catch (std::exception & e) {
		   rodsLog(LOG_ERROR, "%s: bad conversion: %s", __func__, e.what());
           throw;
       }
   }
   return {};
}

/* // SAMPLE USAGE
 *
#include <iostream>
int main (int argc, char** argv)
{
    using json = nlohmann::json;

    json J {
        {"hello", 3.3 },
    };

    if (const auto [_bool, _optional] = kws_get<std::string>(J,"hello"); _bool && _optional)
    {
        std::cout << "yes " << *_optional << std::endl;
    }
    else {
        std::cout << "no  " << std::endl;
        //_optional;
    }
}
 *
 */

#endif // CPP_JSON_KW__HPP
