#ifndef IRODS_CAPABILITY_INDEXING_UTILITIES_HPP
#define IRODS_CAPABILITY_INDEXING_UTILITIES_HPP

#include <irods/irods_re_plugin.hpp>
#include <irods/irods_exception.hpp>
#include <irods/rodsError.h>

#include <boost/any.hpp>
#include <fmt/format.h>

#include <list>
#include <string>
#include <tuple>

namespace irods::indexing
{
	const std::string indexer_separator{"::"};

	inline auto parse_indexer_string(const std::string& _indexer_string) -> std::tuple<std::string, std::string>
	{
		const auto pos = _indexer_string.find_last_of(indexer_separator);
		if (std::string::npos == pos) {
			THROW(SYS_INVALID_INPUT_PARAM,
			      fmt::format("[{}] does not include an index separator for collection", _indexer_string));
		}
		const auto index_name = _indexer_string.substr(0, pos - (indexer_separator.size() - 1));
		const auto index_type = _indexer_string.substr(pos + 1);
		return std::make_tuple(index_name, index_type);
	}

	void exception_to_rerror(const irods::exception& _exception, rError_t& _error);

	void exception_to_rerror(const int _code, const char* _what, rError_t& _error);

	void invoke_policy(ruleExecInfo_t* _rei, const std::string& _action, std::list<boost::any> _args);
} // namespace irods::indexing

#endif // IRODS_CAPABILITY_INDEXING_UTILITIES_HPP
