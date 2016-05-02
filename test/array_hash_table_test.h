#ifndef array_hash_table_test_h
#define array_hash_table_test_h

#include <cppunit/TestFixture.h>
#include <cppunit/TestAssert.h>
#include <cppunit/TestCaller.h>
#include <cppunit/TestSuite.h>
#include "../src/abstract_index.h"
#include "../src/hash_index/array_hash_table.h"
#include "../src/hash_functions/mod_hash.h"
#include <boost/thread.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <thread>
#include <pthread.h>

#define value_t std::uint32_t

namespace dbindex {
	constexpr std::uint8_t initial_bucket_size = 2;
	constexpr std::uint32_t _directory_size = 4;

	void insert_array_concurrent(array_hash_table<std::string, std::string, _directory_size> *hash_table, std::string *keys, std::uint32_t amount) {
		// Calculating the hashing
		for(std::uint32_t j = 0; j < amount; j++) {
			hash_table->insert(keys[j], keys[j]);
		}
	}

	class array_hash_table_test : public CppUnit::TestFixture {
	private:
		abstract_hash<std::uint32_t> *hash;
		array_hash_table<std::string, std::string, _directory_size> *hash_table;
		abstract_push_op *concat_push;

	public:
		array_hash_table_test(){}

		void setUp() {
			std::uint32_t _initial_bucket_size = 2;
			hash = new mod_hash<value_t, (1<<31)>();
			hash_table = new array_hash_table<std::string, std::string, _directory_size>(hash, _initial_bucket_size);
			concat_push = new concat_push_op();
		}

		void tearDown() {
			delete hash;
			delete hash_table;
			delete concat_push;
		}

		void test_insert() {
			std::cout << "TEST_INSERT" << std::endl;
			CPPUNIT_ASSERT(is_table_empty());
			hash_table->insert(" 0", " 1");
			CPPUNIT_ASSERT(!is_table_empty());
			// hash_table->print_array_hash_table();
		}

		void test_delete() {
			std::cout << "TEST_DELETE" << std::endl;
			CPPUNIT_ASSERT(is_table_empty());
			hash_table->insert(" 0", " 1");
			CPPUNIT_ASSERT(!is_table_empty());
			hash_table->remove(" 1");
			CPPUNIT_ASSERT(!is_table_empty());
			hash_table->insert(" 1", " 1");
			hash_table->remove(" 1");
			CPPUNIT_ASSERT(!is_table_empty());
			hash_table->remove(" 0");
			CPPUNIT_ASSERT(is_table_empty());
			// hash_table->print_array_hash_table();
		}

		void test_insert_delete_skew() {
			std::cout << "TEST_INSERT_DELETE_MANY" << std::endl;

			CPPUNIT_ASSERT(is_table_empty());

			std::uint8_t  p = 8;
			for (std::uint64_t i = 0; i < (1<<p)*hash_table->get_directory_size(); i++) {
					hash_table->insert(std::to_string(i*hash_table->get_directory_size()), " 1");
	  		}
			CPPUNIT_ASSERT(hash_table->size() == (1<<p)*hash_table->get_directory_size());
			// hash_table->print_array_hash_table();
			for (std::uint64_t i = 0; i < (1<<p)*hash_table->get_directory_size(); i++)
  			{
				hash_table->remove(std::to_string(i*hash_table->get_directory_size()));
  			}
			CPPUNIT_ASSERT(hash_table->size() == 0);
			// hash_table->print_array_hash_table();
		}

		void test_insert_delete_many() {
			std::cout << "TEST_INSERT_DELETE_MANY" << std::endl;

			CPPUNIT_ASSERT(is_table_empty());

			std::uint8_t  p = 10;
			for (std::uint64_t i = 0; i < (1<<p)*hash_table->get_directory_size(); i++) {
					hash_table->insert(std::to_string(i), " 1");
	  		}
			CPPUNIT_ASSERT(hash_table->size() == (1<<p)*hash_table->get_directory_size());
			// hash_table->print_array_hash_table();
			for (std::uint64_t i = 0; i < (1<<p)*hash_table->get_directory_size(); i++)
  			{
				hash_table->remove(std::to_string(i));
  			}
			CPPUNIT_ASSERT(hash_table->size() == 0);
			// hash_table->print_array_hash_table();
		}

		void test_update() {
			std::cout << "TEST_UPDATE" << std::endl;
			CPPUNIT_ASSERT(hash_table->size() == 0);
			hash_table->insert("1", "1");
			CPPUNIT_ASSERT(hash_table->size() == 1);
			hash_table->update("0", "1");
			CPPUNIT_ASSERT(hash_table->size() == 1);
			std::string a;
			hash_table->get("1", a);
			CPPUNIT_ASSERT(a == "1");
			hash_table->update("1", "2");
			CPPUNIT_ASSERT(hash_table->size() == 1);
			hash_table->get("1", a);
			CPPUNIT_ASSERT(a == "2");
			// hash_table->print_array_hash_table();
		}

		void test_scan() {
			std::cout << "TEST_SCAN" << std::endl;
			hash_table->insert("1", "one");
			hash_table->insert("2", "two");
			hash_table->insert("3", "three");
			hash_table->insert("4", "four");
			hash_table->insert("5", "five");
			const std::string &a = "2";
			const std::string b = "4";
			hash_table->range_scan(a, &b, (*concat_push));
			CPPUNIT_ASSERT(((concat_push_op *)concat_push)->get() == ", two, three, four");
		}
		void test_scan_reverse() {
			std::cout << "TEST_SCAN_REVERSE" << std::endl;
			hash_table->insert("1", "one");
			hash_table->insert("2", "two");
			hash_table->insert("3", "three");
			hash_table->insert("4", "four");
			hash_table->insert("5", "five");
			const std::string &a = "2";
			const std::string b = "4";
			hash_table->reverse_range_scan(a, &b, (*concat_push));
			CPPUNIT_ASSERT(((concat_push_op *)concat_push)->get() == ", four, three, two");
		}

		void test_concurrent_different() {
			std::cout << "TEST_CONCURRENT_DIFFERENT" << std::endl;
			std::uint8_t  num_threads = 4;
			std::uint32_t amount      = 40000;
			std::thread   threads[num_threads];
			std::string   *keys = new std::string[amount*num_threads];

			for(std::uint32_t j = 0; j < num_threads; j++) {
				for(std::uint32_t i = 0; i < amount; i++) {
					keys[j*amount+i] = std::to_string(j+i*num_threads);
				}
			}
			
 			for(std::uint32_t t = 0; t < num_threads; t++) {
				threads[t] = std::thread(insert_array_concurrent, hash_table, &keys[t*amount], amount);
				stick_thread_to_core(threads[t].native_handle(), (t*2)+1);
			}
			for(std::uint32_t t = 0; t < num_threads; t++) {
				threads[t].join();
			}
			CPPUNIT_ASSERT(hash_table->size() == num_threads*amount);
			delete[] keys;
		}

		void test_concurrent_all() {
			std::cout << "TEST_CONCURRENT_ALL" << std::endl;
			std::uint8_t  num_threads = 4;
			std::uint32_t amount      = 1<<16;
			std::thread   threads[num_threads];
			std::string   *keys = new std::string[amount*num_threads];

			for(std::uint32_t i = 0; i < amount*num_threads; i++) {
				keys[i] = std::to_string(i);
			}
			
 			for(std::uint32_t t = 0; t < num_threads; t++) {
				threads[t] = std::thread(insert_array_concurrent, hash_table, &keys[t*amount], amount);
				stick_thread_to_core(threads[t].native_handle(), (t*2)+1);
			}
			for(std::uint32_t t = 0; t < num_threads; t++) {
				threads[t].join();
			}
			CPPUNIT_ASSERT(hash_table->size() == num_threads*amount);
			delete[] keys;
		}

		bool is_table_empty() {
			return (hash_table->size() == 0);
		}

		static CppUnit::Test *suite()
		{
			CppUnit::TestSuite *suite_of_tests = new CppUnit::TestSuite( "array_hash_table_suite" );
			suite_of_tests->addTest( new CppUnit::TestCaller<array_hash_table_test>(
            	           "test_insert",
            	           	&array_hash_table_test::test_insert ) );
			suite_of_tests->addTest( new CppUnit::TestCaller<array_hash_table_test>(
                	       "test_delete",
                    	   &array_hash_table_test::test_delete ) );
			suite_of_tests->addTest( new CppUnit::TestCaller<array_hash_table_test>(
                	       "test_update",
                    	   &array_hash_table_test::test_update ) );

			suite_of_tests->addTest( new CppUnit::TestCaller<array_hash_table_test>(
                       		"test_insert_delete_many",
                       		&array_hash_table_test::test_insert_delete_many ) );
			suite_of_tests->addTest( new CppUnit::TestCaller<array_hash_table_test>(
                       		"test_insert_delete_skew",
                       		&array_hash_table_test::test_insert_delete_skew ) );

			suite_of_tests->addTest( new CppUnit::TestCaller<array_hash_table_test>(
                	       "test_scan",
                    	   &array_hash_table_test::test_scan ) );
			suite_of_tests->addTest( new CppUnit::TestCaller<array_hash_table_test>(
                	       "test_scan_reverse",
                    	   &array_hash_table_test::test_scan_reverse ) );
			
			suite_of_tests->addTest( new CppUnit::TestCaller<array_hash_table_test>(
                       		"test_concurrent_different",
                       		&array_hash_table_test::test_concurrent_different ) );
			suite_of_tests->addTest( new CppUnit::TestCaller<array_hash_table_test>(
                       		"test_concurrent_all",
                       		&array_hash_table_test::test_concurrent_all ) );
			return suite_of_tests;
		};
	};
}
#endif
