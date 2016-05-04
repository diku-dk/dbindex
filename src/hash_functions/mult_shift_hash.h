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
 * mult_shift_hash.h
 *
 *  Created on: May 3, 2016
 *  Author: Vivek Shah <bonii @ di.ku.dk>
 */
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
        constexpr static std::uint8_t shift_by = (sizeof(std::uint64_t)
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
