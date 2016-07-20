#ifndef SRC_HASH_FUNCTIONS_TABULATION_HASH_H_
#define SRC_HASH_FUNCTIONS_TABULATION_HASH_H_

#include <climits>
#include <cstring>
#include <cstdint>
#include <iostream>
#include <algorithm>
#include <assert.h>
#include <memory>
#include "abstract_hash.h"
#include "../macros.h"
#include "random_numbers.h"

namespace dbindex {

    template<typename value_t = std::uint32_t, std::uint8_t max_key_len = 1>
    class tabulation_hash: public abstract_hash<value_t> {
    private:
        alignas(CACHE_LINE_SIZE) value_t tabulation_tables[max_key_len][std::numeric_limits<
                std::uint8_t>::max()];

    public:
        tabulation_hash() {
            static_assert(alignof(tabulation_tables) % sizeof(value_t) == 0, "Tabulation entries can span across cache line");
            fill_random_numbers<value_t,
                    max_key_len * std::numeric_limits<std::uint8_t>::max()>(
                    &(tabulation_tables[0][0]));
        }

        value_t get_hash(const std::string& key) override {
            this->check_key_not_empty(key);
            static_assert(sizeof(std::uint8_t) == sizeof(char), "Char is not equal to uint8_t");
            assert(key.size() <= max_key_len);

            auto ukey = reinterpret_cast<const uint8_t*>(key.c_str());
            value_t hash_result = 0;
            for (std::uint8_t i = 0; i < key.size(); i++) {
                hash_result ^= tabulation_tables[i][ukey[i]];
            }
            return hash_result;
        }
    }
    ;
}
#endif /* SRC_HASH_FUNCTIONS_TABULATION_HASH_H_ */
