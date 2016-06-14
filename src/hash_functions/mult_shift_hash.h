#ifndef SRC_HASH_FUNCTIONS_MULT_SHIFT_HASH_H_
#define SRC_HASH_FUNCTIONS_MULT_SHIFT_HASH_H_

#include <climits>
#include <cstring>
#include <cstdint>
#include <algorithm>
#include <random>
#include <stdexcept>
#include <cassert>
#include <iostream>
#include <chrono>
#include "abstract_hash.h"

namespace dbindex {
    template<typename value_t = std::uint32_t>
    class mult_shift_hash: public abstract_hash<value_t> {
        static constexpr std::uint8_t shift_by = (sizeof(std::uint64_t)
                - sizeof(value_t)) * 8;
    private:
        value_t seed {
                static_cast<value_t>(std::chrono::high_resolution_clock::now().time_since_epoch().count())};

    public:
        mult_shift_hash() {
            seed |= 1;
        }

        value_t get_hash(const std::string& key) override {
            this->check_key_not_empty(key);
            assert(key.size() <= sizeof(char)*8);
            std::uint64_t ukey {0};
            std::copy(&key[0], &key[0] + key.size(),
                    reinterpret_cast<char*>(&ukey));
            return static_cast<value_t>((seed * ukey) >> shift_by);
        }
    };
}
#endif /* SRC_HASH_FUNCTIONS_MULT_SHIFT_HASH_H_ */
