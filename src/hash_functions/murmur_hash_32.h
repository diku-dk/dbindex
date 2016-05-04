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
 * murmur_hash_32.h
 *
 *  Created on: May 3, 2016
 *  Author: Vivek Shah <bonii @ di.ku.dk>
 */
#ifndef SRC_HASH_FUNCTIONS_MURMUR_HASH_32_H_
#define SRC_HASH_FUNCTIONS_MURMUR_HASH_32_H_

#include <climits>
#include <cstring>
#include <cstdint>
#include <algorithm>
#include <random>
#include <stdexcept>
#include <cassert>
#include "abstract_hash.h"

namespace dbindex {
    template<typename value_t = std::uint32_t>
    class murmur_hash_32: public abstract_hash<value_t> {
        static_assert(std::is_same<value_t, std::uint32_t>::value, "Unsupported hash_value");
    private:
        value_t seed {
                static_cast<value_t>(std::chrono::high_resolution_clock::now().time_since_epoch().count()) };

        inline std::uint32_t rotl32(const std::uint32_t& x,
                const std::uint8_t& r) {
            return (x << r) | (x >> (32 - r));
        }

        inline std::uint32_t fmix32(std::uint32_t& h) {
            h ^= h >> 16;
            h *= 0x85ebca6b;
            h ^= h >> 13;
            h *= 0xc2b2ae35;
            h ^= h >> 16;
            return h;
        }

    public:

        murmur_hash_32() {
            seed |= 1;
        }

        value_t get_hash(const std::string& key) override {
            static_assert(sizeof(value_t) == sizeof(std::uint32_t), "value_t must be 32 bit");
            this->check_key_not_empty(key);
            const std::uint8_t *ukey =
                    reinterpret_cast<const std::uint8_t *>(key.c_str());
            const std::uint8_t len = key.size();
            const std::uint32_t nblocks = len / 4;
            value_t h1 = seed;

            const value_t c1 = 0xcc9e2d51;
            const value_t c2 = 0x1b873593;

            //----------
            // body

            const std::uint32_t * blocks = (const std::uint32_t *) (ukey
                    + nblocks * 4);

            for (std::size_t i = 0; i < nblocks; i++) {
                value_t k1 = blocks[i];

                k1 *= c1;
                k1 = rotl32(k1, 15);
                k1 *= c2;

                h1 ^= k1;
                h1 = rotl32(h1, 13);
                h1 = h1 * 5 + 0xe6546b64;
            }

            //----------
            // tail

            const std::uint8_t * tail = (const std::uint8_t*) (ukey
                    + nblocks * 4);

            value_t k1 = 0;

            switch (len & 3) {
                case 3:
                    k1 ^= tail[2] << 16;
                case 2:
                    k1 ^= tail[1] << 8;
                case 1:
                    k1 ^= tail[0];
                    k1 *= c1;
                    k1 = rotl32(k1, 15);
                    k1 *= c2;
                    h1 ^= k1;
            };

            //----------
            // finalization

            h1 ^= len;
            h1 = fmix32(h1);
            return h1;
        }
    };
}
#endif /* SRC_HASH_FUNCTIONS_MURMUR_HASH_32_H_ */

