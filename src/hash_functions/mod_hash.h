#ifndef SRC_HASH_FUNCTIONS_MOD_HASH_H_
#define SRC_HASH_FUNCTIONS_MOD_HASH_H_

#include <cstdint>
#include "abstract_hash.h"

namespace dbindex {
    template<typename value_t = std::uint32_t, std::uint32_t _mod_value = 2>
    class mod_hash : public abstract_hash<value_t> {
    public:
        value_t get_hash(const std::string& key) override {
            this->check_key_not_empty(key);
            value_t ukey = std::atoi(key.c_str());
            if (ukey == 0 && key != "0")
            	ukey = key.size();
            // std::cout << ukey << std::endl;
            return ukey % _mod_value;
        }
    };
}

#endif /* SRC_HASH_FUNCTIONS_MOD_HASH_H_ */
