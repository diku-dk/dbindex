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
 * mod_hash.h
 *
 *  Created on: May 3, 2016
 *  Author: Vivek Shah <bonii @ di.ku.dk>
 */
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
            const value_t ukey = std::atoi(key.c_str());
            return ukey % _mod_value;
        }
    };
}

#endif /* SRC_HASH_FUNCTIONS_MOD_HASH_H_ */
