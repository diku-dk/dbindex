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
 * hash_test.cc
 *
 *  Created on: May 2, 2016
 *  Author: Vivek Shah <bonii @ di.ku.dk>
 */

#include <gtest/gtest.h>
#include <vector>
#include <cstdint>
#include <memory>
#include <utility>
#include <random>
#include <algorithm>
#include <limits>
#include <cmath>
#include <functional>
#include <cmath>
#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include <boost/accumulators/statistics/variance.hpp>
#include <boost/accumulators/accumulators.hpp>
#include <boost/ref.hpp>
#include <boost/accumulators/statistics/skewness.hpp>
#include "../src/hash_functions/mod_hash.h"
#include "../src/hash_functions/mult_shift_hash.h"
#include "../src/hash_functions/murmur_hash_32.h"
#include "../src/hash_functions/tabulation_hash.h"

namespace dbindex {
    namespace test {
        using hash_value_t = std::uint32_t;
        using input_key_t = std::uint64_t;
        static constexpr int MOD_HASH_MOD_VAL = 100;
        static constexpr int MAX_KEY_LEN_VAL = 64;
        static constexpr int NUM_EQUALITY_INPUTS = 20000;
        static constexpr int NUM_EQUALITY_ASSERTS = 10;
        static constexpr int NUM_SPREAD_ITERATIONS = 10;
        static constexpr int NUM_SPREAD_INPUTS = 20000;
        static constexpr int NUM_SPREAD_BUCKETS = 100;
        static constexpr float ACCEPTABLE_DEV_MEAN_RATIO = .1;

        class hash_function_test: public ::testing::Test {
        public:
            void test_spread_of_hash_function(
                    dbindex::abstract_hash<hash_value_t>& hash_fn,
                    std::function<input_key_t()>& input_generator) {
                static_assert(NUM_SPREAD_BUCKETS < NUM_SPREAD_INPUTS, "Buckets must be less than inputs");
                std::vector<std::uint64_t> histogram(NUM_SPREAD_BUCKETS);
                int failed_iterations = 0;
                for (int iteration = 0; iteration < NUM_SPREAD_ITERATIONS;
                        iteration++) {
                    std::fill(histogram.begin(), histogram.end(), 0);
                    for (int i = 0; i < NUM_SPREAD_INPUTS; i++) {
                        auto input_val = input_generator();
                        auto input_val_string = std::string(
                                reinterpret_cast<char*>(&input_val),
                                sizeof(input_val));
                        auto hash_val = hash_fn.get_hash(input_val_string);
                        hash_value_t bucket = hash_val
                                % (hash_value_t) NUM_SPREAD_BUCKETS;
                        histogram[bucket]++;
                    }
                    //Check spread
                    boost::accumulators::accumulator_set<double,
                            boost::accumulators::features<
                                    boost::accumulators::tag::mean,
                                    boost::accumulators::tag::variance>> acc;
                    std::for_each(histogram.begin(), histogram.end(),
                            [&](std::uint64_t& val) {acc(val);});
                    auto mean = boost::accumulators::extract::mean(acc);
                    auto std_dev = std::sqrt(
                            boost::accumulators::extract::variance(acc));
                    if ((float) std_dev / mean > ACCEPTABLE_DEV_MEAN_RATIO) {
                        failed_iterations++;
                    }
                    //Should do a Chi-Squared test
                }

                EXPECT_TRUE(failed_iterations < NUM_SPREAD_ITERATIONS / 2);
            }

            void test_equality_of_hash_function(
                    dbindex::abstract_hash<hash_value_t>& hash_fn,
                    std::function<input_key_t()>& input_generator) {
                std::vector<std::pair<input_key_t, hash_value_t>> inputs(
                        NUM_EQUALITY_INPUTS);
                for (int iteration = 0; iteration < NUM_EQUALITY_ASSERTS;
                        iteration++) {
                    for (int i = 0; i < NUM_EQUALITY_INPUTS; i++) {
                        if (iteration == 0) {
                            //Compute and store
                            auto input_val = input_generator();
                            inputs[i] =
                                    std::make_pair(input_val,
                                            hash_fn.get_hash(
                                                    std::string(
                                                            reinterpret_cast<char*>(&input_val),
                                                            sizeof(input_val))));
                        } else {
                            auto input_val = inputs[i].first;
                            EXPECT_TRUE(
                                    inputs[i].second
                                            == hash_fn.get_hash(
                                                    std::string(
                                                            reinterpret_cast<char*>(&input_val),
                                                            sizeof(input_val))));
                        }
                    }

                }
            }
        };

        TEST_F(hash_function_test, equality_mod_hash) {
            std::default_random_engine generator;
            std::uniform_int_distribution<input_key_t> distribution(
                    std::numeric_limits<input_key_t>::min(),
                    std::numeric_limits<input_key_t>::max());
            std::function<input_key_t()> input_gen_fn { std::bind(distribution,
                    generator) };

            dbindex::mod_hash<hash_value_t, MOD_HASH_MOD_VAL> mod_hash;
            test_equality_of_hash_function(mod_hash, input_gen_fn);

        }

        TEST_F(hash_function_test, equality_mult_shift_hash) {
            std::default_random_engine generator;
            std::uniform_int_distribution<input_key_t> distribution(
                    std::numeric_limits<input_key_t>::min(),
                    std::numeric_limits<input_key_t>::max());
            std::function<input_key_t()> input_gen_fn { std::bind(distribution,
                    generator) };

            dbindex::mult_shift_hash<hash_value_t> mult_shift_hash;
            test_equality_of_hash_function(mult_shift_hash, input_gen_fn);
        }

        /*        TEST_F(hash_function_test, equality_murmur_hash_32) {
         std::default_random_engine generator;
         std::uniform_int_distribution<input_key_t> distribution(
         std::numeric_limits<input_key_t>::min(),
         std::numeric_limits<input_key_t>::max());
         std::function<input_key_t()> input_gen_fn {std::bind(distribution,generator)};

         dbindex::murmur_hash_32<hash_value_t> murmur_hash_32;
         test_equality_of_hash_function(murmur_hash_32, input_gen_fn);
         }
         */
        TEST_F(hash_function_test, equality_tabulation_hash) {
            std::default_random_engine generator;
            std::uniform_int_distribution<input_key_t> distribution(
                    std::numeric_limits<input_key_t>::min(),
                    std::numeric_limits<input_key_t>::max());
            std::function<input_key_t()> input_gen_fn { std::bind(distribution,
                    generator) };
            dbindex::tabulation_hash<hash_value_t, MAX_KEY_LEN_VAL> tabulation_hash;
            test_equality_of_hash_function(tabulation_hash, input_gen_fn);
        }

        TEST_F(hash_function_test, spread_mod_hash) {
            std::default_random_engine generator;
            std::uniform_int_distribution<input_key_t> distribution(
                    std::numeric_limits<input_key_t>::min(),
                    std::numeric_limits<input_key_t>::max());
            std::function<input_key_t()> input_gen_fn { std::bind(distribution,
                    generator) };

            dbindex::mod_hash<hash_value_t, MOD_HASH_MOD_VAL> mod_hash;
            test_spread_of_hash_function(mod_hash, input_gen_fn);
        }

        TEST_F(hash_function_test, spread_mult_shift_hash) {
            std::default_random_engine generator;
            std::uniform_int_distribution<input_key_t> distribution(
                    std::numeric_limits<input_key_t>::min(),
                    std::numeric_limits<input_key_t>::max());
            std::function<input_key_t()> input_gen_fn { std::bind(distribution,
                    generator) };

            dbindex::mult_shift_hash<hash_value_t> mult_shift_hash;
            test_spread_of_hash_function(mult_shift_hash, input_gen_fn);
        }
        /*
         TEST_F(hash_function_test, spread_murmur_hash_32) {
         std::default_random_engine generator;
         std::uniform_int_distribution<input_key_t> distribution(
         std::numeric_limits<input_key_t>::min(),
         std::numeric_limits<input_key_t>::max());
         std::function<input_key_t()> input_gen_fn {std::bind(distribution,generator)};

         dbindex::murmur_hash_32<hash_value_t> murmur_hash_32;
         test_spread_of_hash_function(murmur_hash_32, input_gen_fn);
         }
         */
        TEST_F(hash_function_test, spread_tabulation_hash) {
            std::default_random_engine generator;
            std::uniform_int_distribution<input_key_t> distribution(
                    std::numeric_limits<input_key_t>::min(),
                    std::numeric_limits<input_key_t>::max());
            std::function<input_key_t()> input_gen_fn { std::bind(distribution,
                    generator) };
            dbindex::tabulation_hash<hash_value_t, MAX_KEY_LEN_VAL> tabulation_hash;
            test_spread_of_hash_function(tabulation_hash, input_gen_fn);
        }

    }
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
