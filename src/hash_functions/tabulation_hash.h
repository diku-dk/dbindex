/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2016 Vivek Shah (University of Copenhagen)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:

 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.

 * THE SOFTWARE dbindex IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * tabulation_hash.h
 *
 *  Created on: May 3, 2016
 *  Author: Vivek Shah <bonii @ di.ku.dk>
 */
#ifndef SRC_HASH_FUNCTIONS_TABULATION_HASH_H_
#define SRC_HASH_FUNCTIONS_TABULATION_HASH_H_

#include <climits>
#include <cstring>
#include <cstdint>
#include <algorithm>
#include <memory>
#include "abstract_hash.h"
#include "../macros.h"
#include "random_numbers.h"

namespace dbindex {
    template<typename value_t = std::uint32_t, std::uint8_t num_tables = 1>
    class tabulation_hash: public abstract_hash<value_t> {
    private:
        alignas(CACHE_LINE_SIZE) value_t tabulation_tables[num_tables][std::numeric_limits<
                std::uint8_t>::max()];

    public:
        tabulation_hash() {
            fill_random_numbers<value_t,
                    num_tables * std::numeric_limits<std::uint8_t>::max()>(
                    &(tabulation_tables[0][0]));
        }

        value_t get_hash(const std::string& key) override {
            this->check_key_not_empty(key);
            static_assert(sizeof(std::uint8_t) == sizeof(char), "Char is not equal to uint8_t");

            auto ukey = reinterpret_cast<const uint8_t*>(key.c_str());
            value_t hash_result = 0;
            for (std::uint8_t i = 0; i < key.size(); i++) {
                hash_result ^= tabulation_tables[i % num_tables][ukey[i]];
            }
            return hash_result;
        }
    }
    ;
}
#endif /* SRC_HASH_FUNCTIONS_TABULATION_HASH_H_ */
