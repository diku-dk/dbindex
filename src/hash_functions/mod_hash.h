#ifndef mod_hash_h
#define mod_hash_h

#include <cstdint>
#include "abstract_hash.h"

namespace dbindex {
    template<typename value_t = std::uint32_t, std::uint32_t _mod_value = 2>
    class mod_hash: public abstract_hash<value_t> {
    public:
        value_t get_hash(const std::string& key) override {
            const value_t ukey = std::atoi(key.c_str());
            // std::cout << "ModHash: " << skey << ", " << ukey << std::endl;
            return ukey % _mod_value;
        }
    };
}

#endif
