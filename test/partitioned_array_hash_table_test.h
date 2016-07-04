#ifndef TEST_PARTITIONED_ARRAY_HASH_TABLE_TEST_H
#define TEST_PARTITIONED_ARRAY_HASH_TABLE_TEST_H

#include <cppunit/TestFixture.h>
#include <cppunit/TestAssert.h>
#include <cppunit/TestCaller.h>
#include <cppunit/TestSuite.h>
#include "../src/abstract_index.h"
#include "../src/hash_index/partitioned_array_hash_table.h"
#include "../src/hash_functions/mod_hash.h"
#include "../src/push_ops.h"
#include "../src/util/thread_util.h"
#include <boost/thread.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <thread>

namespace dbindex {
	constexpr std::uint32_t directory_size = 4;
	constexpr std::uint8_t prefix_bits = 4;

	class partitioned_array_hash_table_test : public common_hash_table_test {
	private:
		mod_hash<hash_value_t, (1<<31)> hash{};
		partitioned_array_hash_table<prefix_bits, directory_size> hash_table{hash};
		concat_push_op concat_push{};

	public:
		partitioned_array_hash_table_test() : common_hash_table_test(hash, hash_table){}


		void test_insert_delete_many() {
			std::cout << "TEST_INSERT_DELETE_MANY" << std::endl;

			CPPUNIT_ASSERT(is_table_empty());

			std::uint8_t  p = 10;
			for (std::uint64_t i = 0; i < (1<<p)*hash_table.get_directory_size(); i++) {
					hash_table.insert(std::to_string(i+(1<<8)), "11");
	  		}
			CPPUNIT_ASSERT(hash_table.size() == (1<<p)*hash_table.get_directory_size());
			// hash_table.print_array_hash_table();
			for (std::uint64_t i = 0; i < (1<<p)*hash_table.get_directory_size(); i++)
  			{
				hash_table.remove(std::to_string(i+(1<<8)));
  			}
			CPPUNIT_ASSERT(hash_table.size() == 0);
			// hash_table.print_array_hash_table();
		}

		void test_insert_delete_skew() {
			std::cout << "TEST_INSERT_DELETE_SKEW" << std::endl;

			CPPUNIT_ASSERT(is_table_empty());

			std::uint8_t  p = 8;
			for (std::uint64_t i = 0; i < (1<<p)*hash_table.get_directory_size(); i++) {
				hash_table.insert(std::to_string(i*hash_table.get_directory_size()+(1<<8)), "11");
	  		}
			CPPUNIT_ASSERT(hash_table.size() == (1<<p)*hash_table.get_directory_size());
			// hash_table.print_array_hash_table();
			for (std::uint64_t i = 0; i < (1<<p)*hash_table.get_directory_size(); i++)
  			{
				hash_table.remove(std::to_string(i*hash_table.get_directory_size()+(1<<8)));
  			}
			CPPUNIT_ASSERT(hash_table.size() == 0);
		}

		static CppUnit::Test* suite()
		{
			CppUnit::TestSuite* suite_of_tests = new CppUnit::TestSuite( "partitioned_array_hash_table_suite" );
			suite_of_tests->addTest( new CppUnit::TestCaller<partitioned_array_hash_table_test>(
            	           "test_insert",
            	           	&partitioned_array_hash_table_test::test_insert ) );
			suite_of_tests->addTest( new CppUnit::TestCaller<partitioned_array_hash_table_test>(
                	       "test_delete",
                    	   &partitioned_array_hash_table_test::test_delete ) );
			suite_of_tests->addTest( new CppUnit::TestCaller<partitioned_array_hash_table_test>(
                	       "test_update",
                    	   &partitioned_array_hash_table_test::test_update ) );

			suite_of_tests->addTest( new CppUnit::TestCaller<partitioned_array_hash_table_test>(
                       		"test_insert_delete_many",
                       		&partitioned_array_hash_table_test::test_insert_delete_many ) );
			suite_of_tests->addTest( new CppUnit::TestCaller<partitioned_array_hash_table_test>(
                       		"test_insert_delete_skew",
                       		&partitioned_array_hash_table_test::test_insert_delete_skew ) );

			suite_of_tests->addTest( new CppUnit::TestCaller<partitioned_array_hash_table_test>(
                	       "test_scan",
                    	   &partitioned_array_hash_table_test::test_scan ) );
			// suite_of_tests->addTest( new CppUnit::TestCaller<partitioned_array_hash_table_test>(
   //              	       "test_scan_reverse",
   //                  	   &partitioned_array_hash_table_test::test_scan_reverse ) );
			
			suite_of_tests->addTest( new CppUnit::TestCaller<partitioned_array_hash_table_test>(
                       		"test_concurrent_different",
                       		&partitioned_array_hash_table_test::test_concurrent_different ) );
			suite_of_tests->addTest( new CppUnit::TestCaller<partitioned_array_hash_table_test>(
                       		"test_concurrent_all",
                       		&partitioned_array_hash_table_test::test_concurrent_all ) );
			suite_of_tests->addTest( new CppUnit::TestCaller<partitioned_array_hash_table_test>(
                       		"test_concurrent_updates_known",
                       		&partitioned_array_hash_table_test::test_concurrent_updates_known ) );
			suite_of_tests->addTest( new CppUnit::TestCaller<partitioned_array_hash_table_test>(
                       		"test_concurrent_scans",
                       		&partitioned_array_hash_table_test::test_concurrent_scans ) );
			return suite_of_tests;
		};
	};
}
#endif /* TEST_PARTITIONED_ARRAY_HASH_TABLE_TEST_H */
