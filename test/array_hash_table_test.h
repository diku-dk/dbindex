#ifndef array_hash_table_test_h
#define array_hash_table_test_h

#include <cppunit/TestFixture.h>
#include <cppunit/TestAssert.h>
#include <cppunit/TestCaller.h>
#include <cppunit/TestSuite.h>
#include "../src/abstract_index.h"
#include "../src/hash_index/array_hash_table.h"
#include "../src/hash_functions/mod_hash.h"
#include "../src/push_ops.h"
#include "../src/util/thread_util.h"
#include <boost/thread.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <thread>

namespace dbindex {
	constexpr std::uint8_t initial_bucket_size = 2;
	constexpr std::uint32_t _directory_size = 4;

	void insert_array_concurrent(array_hash_table<_directory_size>& hash_table, std::string *keys, std::uint32_t amount) {
		// Calculating the hashing
		for(std::uint32_t j = 0; j < amount; j++) {
			hash_table.insert(keys[j], keys[j]);
		}
	}

	class array_hash_table_test : public common_hash_table_test {
	private:
		mod_hash<hash_value_t, (1<<31)> hash{};
		array_hash_table<_directory_size> hash_table{hash};
		concat_push_op concat_push{};

	public:
		array_hash_table_test() : common_hash_table_test(hash, hash_table){}

		void test_insert_delete_many() {
			std::cout << "TEST_INSERT_DELETE_MANY" << std::endl;

			CPPUNIT_ASSERT(is_table_empty());

			std::uint8_t  p = 10;
			for (std::uint64_t i = 0; i < (1<<p)*hash_table.get_directory_size(); i++) {
					hash_table.insert(std::to_string(i), " 1");
	  		}
			CPPUNIT_ASSERT(hash_table.size() == (1<<p)*hash_table.get_directory_size());
			// hash_table.print_array_hash_table();
			for (std::uint64_t i = 0; i < (1<<p)*hash_table.get_directory_size(); i++)
  			{
				hash_table.remove(std::to_string(i));
  			}
			CPPUNIT_ASSERT(hash_table.size() == 0);
			// hash_table.print_array_hash_table();
		}

		void test_insert_delete_skew() {
			std::cout << "TEST_INSERT_DELETE_SKEW" << std::endl;

			CPPUNIT_ASSERT(is_table_empty());

			std::uint8_t  p = 8;
			for (std::uint64_t i = 0; i < (1<<p)*hash_table.get_directory_size(); i++) {
					hash_table.insert(std::to_string(i*hash_table.get_directory_size()), " 1");
	  		}
			CPPUNIT_ASSERT(hash_table.size() == (1<<p)*hash_table.get_directory_size());
			// hash_table.print_array_hash_table();
			for (std::uint64_t i = 0; i < (1<<p)*hash_table.get_directory_size(); i++)
  			{
				hash_table.remove(std::to_string(i*hash_table.get_directory_size()));
  			}
			CPPUNIT_ASSERT(hash_table.size() == 0);
		}

		static CppUnit::Test* suite()
		{
			CppUnit::TestSuite* suite_of_tests = new CppUnit::TestSuite( "array_hash_table_suite" );
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
                       		"test_concurrent_different",
                       		&array_hash_table_test::test_concurrent_different ) );
			suite_of_tests->addTest( new CppUnit::TestCaller<array_hash_table_test>(
                       		"test_concurrent_all",
                       		&array_hash_table_test::test_concurrent_all ) );
			suite_of_tests->addTest( new CppUnit::TestCaller<array_hash_table_test>(
                       		"test_concurrent_updates_known",
                       		&array_hash_table_test::test_concurrent_updates_known ) );
			suite_of_tests->addTest( new CppUnit::TestCaller<array_hash_table_test>(
                       		"test_concurrent_scans",
                       		&array_hash_table_test::test_concurrent_scans ) );
			return suite_of_tests;
		};
	};
}
#endif
