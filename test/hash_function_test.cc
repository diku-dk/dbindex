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

#include <cppunit/TextTestRunner.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestAssert.h>
#include <cppunit/TestCaller.h>
#include <cppunit/TestSuite.h>
#include <cppunit/TestResult.h>
#include <cppunit/TestResultCollector.h>
#include <cppunit/TextTestProgressListener.h>
#include <vector>
#include <cstdint>
#include <memory>
#include <utility>
#include <random>
#include <limits>
#include <cmath>
#include "../src/hash_functions/mod_hash.h"
#include "../src/hash_functions/mult_shift_hash.h"
#include "../src/hash_functions/murmur_hash_32.h"
#include "../src/hash_functions/tabulation_hash.h"
#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include <boost/accumulators/statistics/variance.hpp>
#include <boost/accumulators/accumulators.hpp>
#include <boost/ref.hpp>
#include <boost/accumulators/statistics/skewness.hpp>

namespace dbindex {
    namespace test {
        class hash_function_test: public CppUnit::TestFixture {
            using hash_value_t = std::uint32_t;
            using input_key_t = std::uint64_t;
            static constexpr int MOD_HASH_MOD_VAL = 100;
            static constexpr int MAX_KEY_LEN_VAL = 64;
            static constexpr int NUM_EQUALITY_INPUTS = 100;
            static constexpr int NUM_EQUALITY_ASSERTS = 5;
            static constexpr int NUM_SPREAD_ITERATIONS = 10;
            static constexpr int NUM_SPREAD_INPUTS = 20000;
            static constexpr int NUM_SPREAD_BUCKETS = 100;
            static constexpr float ACCEPTABLE_DEV_MEAN_RATIO = .1;
        private:
            std::vector<std::unique_ptr<dbindex::abstract_hash<hash_value_t>>>hash_functions;

    public:
        void setUp() {
            // hash_functions.emplace_back(std::unique_ptr<dbindex::abstract_hash<hash_value_t>>(new dbindex::mod_hash<hash_value_t,MOD_HASH_MOD_VAL>()));
            // hash_functions.emplace_back(std::unique_ptr<dbindex::abstract_hash<hash_value_t>>(new dbindex::mult_shift_hash<hash_value_t>()));
            hash_functions.emplace_back(std::unique_ptr<dbindex::abstract_hash<hash_value_t>>(new dbindex::murmur_hash_32<hash_value_t>()));
            hash_functions.emplace_back(std::unique_ptr<dbindex::abstract_hash<hash_value_t>>(new dbindex::tabulation_hash<hash_value_t, MAX_KEY_LEN_VAL>()));
        }

        void tearDown() {

        }

        void test_equality() {
            std::vector<std::pair<input_key_t,hash_value_t>> inputs {NUM_EQUALITY_INPUTS};
            std::default_random_engine generator;
            std::uniform_int_distribution<input_key_t> distribution(std::numeric_limits<input_key_t>::min(), std::numeric_limits<input_key_t>::max());
            for(auto& a_hash_fn : hash_functions) {
                for(int iteration = 0; iteration < NUM_EQUALITY_ASSERTS; iteration++) {
                    for(int i=0;i<NUM_EQUALITY_INPUTS;i++) {
                        if(iteration == 0) {
                            //Compute and store
                            auto input_val = distribution(generator);
                            inputs[i] = std::make_pair(input_val, a_hash_fn->get_hash(std::string(reinterpret_cast<char*>(&input_val), sizeof(input_val))));
                        } else {
                            auto input_val = inputs[i].first;
                            CPPUNIT_ASSERT(inputs[i].second == a_hash_fn->get_hash(std::string(reinterpret_cast<char*>(&input_val), sizeof(input_val))));
                        }
                    }

                }
            }
        }

        void test_spread() {
            static_assert(NUM_SPREAD_BUCKETS < NUM_SPREAD_INPUTS, "Buckets must be less than inputs");
            std::default_random_engine generator;
            std::uniform_int_distribution<input_key_t> distribution(std::numeric_limits<input_key_t>::min(), std::numeric_limits<input_key_t>::max());
            for(auto& hash_fn : hash_functions) {
                std::vector<std::uint64_t> histogram(NUM_SPREAD_BUCKETS);
                int failed_iterations = 0;
                for (int iteration = 0; iteration < NUM_SPREAD_ITERATIONS; iteration++) {
                    std::fill(histogram.begin(), histogram.end(), 0);
                    for (int i = 0; i < NUM_SPREAD_INPUTS; i++) {
                        auto input_val = distribution(generator);
                        auto input_val_string = std::string(reinterpret_cast<char*>(&input_val), sizeof(input_val));
                        auto hash_val = hash_fn->get_hash(input_val_string);
                        hash_value_t bucket = hash_val % (hash_value_t) NUM_SPREAD_BUCKETS;
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

                CPPUNIT_ASSERT(failed_iterations < NUM_SPREAD_ITERATIONS / 2);
            }
        }

        static CppUnit::Test *suite() {
            CppUnit::TestSuite *suite_of_tests = new CppUnit::TestSuite(
                    "hash_function_test");

            suite_of_tests->addTest(new CppUnit::TestCaller<hash_function_test>(
                          "test_equality",
                          &hash_function_test::test_equality));
            suite_of_tests->addTest(new CppUnit::TestCaller<hash_function_test>(
                          "test_spread",
                          &hash_function_test::test_spread));
            CppUnit::TestResult result;
            return suite_of_tests;
        }
    };
}
}

int main(int argc, char **argv) {
    CppUnit::TextTestRunner runner;
    runner.addTest(dbindex::test::hash_function_test::suite());
    runner.run();
    return 0;
}
