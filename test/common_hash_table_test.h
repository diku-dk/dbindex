#ifndef TEST_COMMON_HASH_TABLE_TEST_H
#define TEST_COMMON_HASH_TABLE_TEST_H

#include <cppunit/TestFixture.h>
#include <cppunit/TestAssert.h>
#include <cppunit/TestCaller.h>
#include <cppunit/TestSuite.h>
#include "../src/push_ops.h"
#include "../src/util/thread_util.h"
#include <boost/thread.hpp>
#include <thread>
#include <random>

typedef std::uint32_t value_t;

namespace dbindex {
	// Concurrent Operations:

	void insert_concurrent(abstract_index& hash_table, std::string *keys, std::uint32_t amount) {
		// Calculating the hashing
		for(std::uint32_t j = 0; j < amount; j++) {
			hash_table.insert(keys[j], keys[j]);
		}
	}

	void insert_concurrent_fixed(abstract_index& hash_table, std::string *keys, std::uint32_t amount, std::string value) {
		// Calculating the hashing
		for(std::uint32_t j = 0; j < amount; j++) {
			hash_table.insert(keys[j], value);
		}
	}

	/* Expecting to get a value from 'valid_values' on all 'get' operations */
	/* The keys are expected to be consecutive                              */
	void read_concurrent_random(abstract_index& hash_table, std::string *keys, std::uint32_t amount, std::vector<std::string> valid_values, bool& is_valid) {
		std::random_device rd;
        std::mt19937 generator(rd());
        std::uniform_int_distribution<value_t> distribution(0, amount-1);

		// Calculating the hashing
		std::string read_value;
		for(std::uint32_t j = 0; j < amount; j++) {
			is_valid &= hash_table.get(keys[distribution(generator)], read_value); // Check that a value was found
			is_valid &= (std::find(valid_values.begin(), valid_values.end(), read_value) != valid_values.end()); // Check that the value is in 'valid_values'
		}	
	}

	/* Expecting to fail in all 'get' operations */
	void read_concurrent_out_of_range(abstract_index& hash_table, std::uint32_t amount, std::uint32_t first_value, std::uint32_t last_value, bool& is_valid) {
		std::random_device rd;
        std::mt19937 generator(rd());
        std::uniform_int_distribution<value_t> distribution(first_value, last_value-1);

		// Calculating the hashing
		std::string read_value;
		for(std::uint32_t j = 0; j < amount; j++) {
			is_valid &= !(hash_table.get(std::to_string(distribution(generator)), read_value)); // Check that a value was NOT found
		}	
	}

	void update_concurrent_from_vec(abstract_index& hash_table, std::string *keys, std::uint32_t amount, std::vector<std::string> valid_values) {
        std::random_device rd;
        std::mt19937 generator(rd());
        std::uniform_int_distribution<value_t> distribution(0, valid_values.size()-1);
        // Calculating the hashing
		for(std::uint32_t j = 0; j < amount; j++) {
			hash_table.update(keys[j], valid_values[distribution(generator)]);
		}
	}

	void scan_concurrent_random_from_vec(abstract_index& hash_table, std::vector<std::string> keys, std::uint32_t amount, std::vector<std::string> valid_values, bool& is_valid) {
		std::random_device rd;
        std::mt19937 generator(rd());
        std::uniform_int_distribution<value_t> lower_distribution(0, keys.size()/2-1);
        std::uniform_int_distribution<value_t> higher_distribution(keys.size()/2, keys.size()-1);

        vector_push_op po{};

        std::sort(keys.begin(), keys.end());

		// Calculating the hashing
		for(std::uint32_t j = 0; j < amount; j++) {
			std::string& first_key = keys[lower_distribution(generator)];
			std::string  last_key  = keys[higher_distribution(generator)];
			hash_table.range_scan(first_key, &last_key, po);
			for (auto value : po.get()) {
				is_valid &= (std::find(valid_values.begin(), valid_values.end(), value) != valid_values.end()); // Check that the value is in the valid valuess
			}
		}	
	}

	class concat_push_op : public abstract_push_op {
	private:
		std::string concat_result = "";
    public:
        ~concat_push_op() {}
        
        bool invoke(const char *keyp, size_t keylen, const std::string &value) {
        	concat_result += ", " + value;
        	return true;
        }
        std::string get() {
        	return concat_result;
        }
    };

	class common_hash_table_test : public CppUnit::TestFixture {
	private:
		abstract_hash<std::uint32_t>& hash;
		abstract_index& hash_table;
		concat_push_op concat_push;

	public:
		common_hash_table_test(abstract_hash<std::uint32_t>& _hash, abstract_index& _hash_table) : hash(_hash), hash_table(_hash_table) {
		}

		void setUp() {
		}

		void tearDown() {
		}


		void test_insert() {
			// std::cout << hash_table.to_string() << std::endl;
			std::cout << "TEST_INSERT" << std::endl;
			CPPUNIT_ASSERT(is_table_empty());
			hash_table.insert("01", "10");
			CPPUNIT_ASSERT(!is_table_empty());
			CPPUNIT_ASSERT(hash_table.size() == 1);
		}


		void test_delete() {
			std::cout << "TEST_DELETE" << std::endl;
			CPPUNIT_ASSERT(is_table_empty());
			hash_table.insert("00", "10");
			CPPUNIT_ASSERT(!is_table_empty());
			CPPUNIT_ASSERT(hash_table.size() == 1);
			hash_table.remove("10");
			CPPUNIT_ASSERT(!is_table_empty());
			hash_table.insert("10", "10");
			CPPUNIT_ASSERT(hash_table.size() == 2);
			hash_table.remove("10");
			CPPUNIT_ASSERT(hash_table.size() == 1);
			CPPUNIT_ASSERT(!is_table_empty());
			hash_table.remove("00");
			CPPUNIT_ASSERT(is_table_empty());
			CPPUNIT_ASSERT(hash_table.size() == 0);
		}


		void test_insert_delete_many() {
			std::cout << "TEST_INSERT_DELETE_MANY" << std::endl;
			CPPUNIT_ASSERT(hash_table.size() == 0);
			std::uint64_t i = 0;
			std::uint8_t  p = 12;
			for (std::uint8_t i = 1; i <= (std::uint32_t)1<<p; i++) {
				hash_table.insert(std::to_string(i+(1<<8)), "10");
			}
			CPPUNIT_ASSERT(hash_table.size() == (std::uint32_t)1<<p);
			for (i = 0; i < (std::uint32_t)1<<p; i++)
			{
				hash_table.remove(std::to_string(i+(1<<8)));
			}
			CPPUNIT_ASSERT(hash_table.size() == 0);
		}


		void test_update() {
			std::cout << "TEST_UPDATE" << std::endl;
			CPPUNIT_ASSERT(hash_table.size() == 0);
			hash_table.insert("10", "10");
			CPPUNIT_ASSERT(hash_table.size() == 1);
			hash_table.update("00", "10");
			CPPUNIT_ASSERT(hash_table.size() == 1);
			std::string a;
			hash_table.get("10", a);
			CPPUNIT_ASSERT(a == "10");
			hash_table.update("10", "20");
			CPPUNIT_ASSERT(hash_table.size() == 1);
			hash_table.get("10", a);
			CPPUNIT_ASSERT(a == "20");
		}


		void test_scan() {
			std::cout << "TEST_SCAN" << std::endl;
			std::uint32_t key_amount = 1000;
			std::uint32_t iterations =  100;
			std::vector<std::string> keys{key_amount};
			std::vector<std::string> values{key_amount};
			
            std::default_random_engine generator;
            std::uniform_int_distribution<std::uint32_t> lower_key_distribution{1, (key_amount/2)-1};
            std::uniform_int_distribution<std::uint32_t> higher_key_distribution{key_amount/2, key_amount-1};
            std::uniform_int_distribution<std::uint32_t> value_distribution{std::numeric_limits<uint32_t>::min(), std::numeric_limits<uint32_t>::max()};

			for(std::uint32_t i = 0; i < key_amount; i++) {
                auto input_val = value_distribution(generator);
                keys[i] = std::string(i, 0xFF) + std::to_string(i);
                values[i] = std::string(reinterpret_cast<char*>(&input_val), sizeof(input_val));
                hash_table.insert(keys[i], values[i]);
			}

			for(std::uint32_t i = 0; i < iterations; i++) {
				vector_push_op po{};

				// Checking range_scan
				std::uint32_t lower_key_int = lower_key_distribution(generator);
				std::uint32_t higher_key_int = higher_key_distribution(generator);

				const std::string& start_key = keys[lower_key_int];
				const std::string end_key = keys[higher_key_int];

					// Calculating the expected result
				std::vector<std::string>::const_iterator first = values.begin() + lower_key_int;
				std::vector<std::string>::const_iterator last = values.begin() + higher_key_int+1;
				std::vector<std::string> expected_result(first, last);
				
					// Calculating the actual result
				hash_table.range_scan(start_key, &end_key, po);

				if (po.get() != expected_result) {
					std::cout << "Problem at iteration: " << i << std::endl;
					auto result = po.get();
					std::cout << "Result lengths: " << result.size() << ", " <<expected_result.size() << std::endl;
	                for (std::uint32_t i = 0; i < expected_result.size(); i++) {
	                	if (result[i] != expected_result[i]){}
	                		// std::cout << "Lengths: " << result[i] << ", " << expected_result[i] << std::endl;
	                }
				}
					// Checking for equality
				CPPUNIT_ASSERT(po.get() == expected_result);

				po.clear();
				// Checking the reverse_range_scan
					// Calculating the actual result
				hash_table.reverse_range_scan(start_key, &end_key, po);

					// Calculating the expected result
				std::reverse(expected_result.begin(), expected_result.end());
				if (po.get() != expected_result) {
					std::cout << "Problem at iteration: " << i << std::endl;
					auto result = po.get();
					std::cout << "Result lengths: " << result.size() << ", " <<expected_result.size() << std::endl;
	                for (std::uint32_t i = 0; i < expected_result.size(); i++) {
	                	if (result[i] != expected_result[i]){}
	                		// std::cout << "Lengths: " << result[i] << ", " << expected_result[i] << std::endl;
	                }
				}

					// Checking for equality
				CPPUNIT_ASSERT(po.get() == expected_result);
			}

		}

		void test_concurrent_different() {
			std::cout << "TEST_CONCURRENT_DIFFERENT" << std::endl;
			std::uint8_t  num_threads = 4;
			std::uint32_t amount      = 40000;
			std::thread   threads[num_threads];
			std::vector<std::string> keys{amount*num_threads};

			for(std::uint32_t j = 0; j < num_threads; j++) {
				for(std::uint32_t i = 0; i < amount; i++) {
					keys[j*amount+i] = std::to_string(j+i*num_threads+(1<<8));
				}
			}
			
			for(std::uint32_t t = 0; t < num_threads; t++) {
				threads[t] = std::thread(insert_concurrent, std::ref(hash_table), &keys[t*amount], amount);
				utils::stick_thread_to_core(threads[t].native_handle(), (t*2)+1);
			}
			for(std::uint32_t t = 0; t < num_threads; t++) {
				threads[t].join();
			}
			CPPUNIT_ASSERT(hash_table.size() == num_threads*amount);
		}

		void test_concurrent_all() {
			std::cout << "TEST_CONCURRENT_ALL" << std::endl;
			std::uint8_t  num_threads = 4;
			std::uint32_t amount      = 1<<16;
			std::thread   threads[num_threads];
			std::vector<std::string> keys{amount*num_threads};

			for(std::uint32_t i = 0; i < amount*num_threads; i++) {
				keys[i] = std::to_string(i+(1<<8));
			}
			
			for(std::uint32_t t = 0; t < num_threads; t++) {
				threads[t] = std::thread(insert_concurrent, std::ref(hash_table), &keys[t*amount], amount);
				utils::stick_thread_to_core(threads[t].native_handle(), (t*2)+1);
			}
			for(std::uint32_t t = 0; t < num_threads; t++) {
				threads[t].join();
			}
			CPPUNIT_ASSERT(hash_table.size() == num_threads*amount);
		}

		void test_concurrent_updates_known() {
			std::cout << "TEST_CONCURRENT_UPDATES_KNOWN" << std::endl;
			std::uint8_t  num_threads  = 1<<2;
			std::uint32_t pre_amount   = 1<<10;
			std::uint32_t first_value  = 1<<8;
			std::thread   threads[num_threads];
			std::string   initial_value = "255";
			std::vector<std::string> keys{pre_amount*num_threads};
			std::vector<std::string> valid_values{1<<5};
			
			bool is_valid = true; // Checks if all reads of updated values are valid

			for(std::uint32_t i = 0; i < pre_amount*(num_threads-1); i++) {
				keys[i] = std::to_string(i+first_value);
			}
			for(std::uint32_t i = 0; i < (1<<5); i++) {
				valid_values[i] = std::to_string(i*4+first_value);
			}
			valid_values.push_back(initial_value);
			// Initial insertion phase - Concurrent faster tests
			for(std::uint8_t t = 0; t < num_threads-1U; t++) {
				threads[t] = std::thread(insert_concurrent_fixed, std::ref(hash_table), &keys[t*pre_amount], pre_amount, initial_value);
				utils::stick_thread_to_core(threads[t].native_handle(), (t*2)+1);
			}
			for(std::uint8_t t = 0; t < num_threads-1U; t++) {
				threads[t].join();
			}
			
			// Actual updating and reading: num_threads-1 update threads, 1 read thread
			for(std::uint8_t t = 1; t < num_threads; t++) {
				threads[t] = std::thread(update_concurrent_from_vec, std::ref(hash_table), &keys[(t-1)*pre_amount], pre_amount, valid_values);
				utils::stick_thread_to_core(threads[t].native_handle(), (t*2)+1);
			}
			// Reader thread
			threads[0] = std::thread(read_concurrent_random, std::ref(hash_table), &keys[0], pre_amount*(num_threads-1), valid_values, std::ref(is_valid));
			utils::stick_thread_to_core(threads[0].native_handle(), 1);

			for(std::uint8_t t = 0; t < num_threads; t++) {
				threads[t].join();
			}

			vector_push_op po{};

			std::string first_key = "";

			hash_table.range_scan(first_key, nullptr, po);
			for (auto value : po.get()) {
				is_valid &= (std::find(valid_values.begin(), valid_values.end(), value) != valid_values.end());
			}

			CPPUNIT_ASSERT(is_valid);
			CPPUNIT_ASSERT(hash_table.size() == (num_threads-1)*pre_amount);
		}

		void test_concurrent_scans() {
			std::cout << "TEST_CONCURRENT_SCANS" << std::endl;
			std::uint8_t  num_threads  = 1<<2;
			std::uint32_t pre_amount   = 1<<10;
			std::uint32_t scan_amount   = 1<<5;
			std::uint32_t first_value  = 1<<8;
			std::thread   threads[num_threads];
			std::string   initial_value = "255";
			std::vector<std::string> keys{pre_amount*num_threads};
			std::vector<std::string> valid_values{1<<5};
			
			bool is_valid = true; // Checks if all reads of updated values are valid

			for(std::uint32_t i = 0; i < pre_amount*num_threads; i++) {
				keys[i] = std::to_string(i+first_value);
			}
			for(std::uint32_t i = 0; i < (1<<5); i++) {
				valid_values[i] = std::to_string(i*4+first_value);
			}
			valid_values.push_back(initial_value);
			// Initial insertion phase - Concurrent faster tests
			for(std::uint8_t t = 0; t < num_threads; t++) {
				threads[t] = std::thread(insert_concurrent_fixed, std::ref(hash_table), &keys[t*pre_amount], pre_amount, initial_value);
				utils::stick_thread_to_core(threads[t].native_handle(), (t*2)+1);
			}
			for(std::uint8_t t = 0; t < num_threads; t++) {
				threads[t].join();
			}
			
			// Actual updating and reading: num_threads-1 update threads, 1 read thread
			for(std::uint8_t t = 0; t < num_threads; t++) {
				std::vector<std::string>::const_iterator first = keys.begin() + t*pre_amount;
				std::vector<std::string>::const_iterator last =  keys.begin() + (t+1)*pre_amount;
				std::vector<std::string> used_keys(first, last);
				threads[t] = std::thread(scan_concurrent_random_from_vec, std::ref(hash_table), used_keys, scan_amount, valid_values, std::ref(is_valid));
				utils::stick_thread_to_core(threads[t].native_handle(), (t*2)+1);
			}

			for(std::uint8_t t = 0; t < num_threads; t++) {
				threads[t].join();
			}

			CPPUNIT_ASSERT(is_valid);
			CPPUNIT_ASSERT(hash_table.size() == num_threads*pre_amount);
		}

		bool is_table_empty() {
			return hash_table.size() == 0;
		}

		// virtual static CppUnit::Test *suite() = 0;
	};
}

#endif  /* TEST_COMMON_HASH_TABLE_TEST_H */