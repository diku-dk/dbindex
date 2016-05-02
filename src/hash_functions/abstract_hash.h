#ifndef abstract_hash_h
#define abstract_hash_h

#include <string>

namespace multicore_hash {
	template<typename value_t>
	class abstract_hash
	{
	public:
		virtual value_t get_hash(const std::string) = 0;
	};
}

#endif