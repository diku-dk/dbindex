#ifndef TEST_EXTENDIBLE_HASH_TABLE_TEST_H_
#define TEST_EXTENDIBLE_HASH_TABLE_TEST_H_

#include <cppunit/TestFixture.h>
#include <cppunit/TestAssert.h>
#include <cppunit/TestCaller.h>
#include <cppunit/TestSuite.h>
#include "../src/abstract_index.h"
#include "../test/common_hash_table_test.h"
#include "../src/hash_index/extendible_hash_table.h"
#include "../src/hash_functions/mod_hash.h"
#include "../src/push_ops.h"
#include "../src/util/thread_util.h"
#include <boost/thread.hpp>
#include <thread>


namespace dbindex {

	constexpr std::uint8_t initial_global_depht = 2;

	class extendible_hash_table_test : public common_hash_table_test {
	private:
		mod_hash<hash_value_t, (1<<31)> hash{};
		extendible_hash_table<initial_global_depht> hash_table{hash};
		concat_push_op concat_push{};

	public:
		extendible_hash_table_test() : common_hash_table_test(hash, hash_table) {}

		void setUp() {
		}

		void tearDown() {
		}

		void test_split() {
			std::cout << "TEST_SPLIT" << std::endl;
			// No split from start
			CPPUNIT_ASSERT(hash_table.get_global_depth() == 2);

			// Filling one bucket doesn't split
			for (std::uint32_t i = 0; i < hash_table.get_bucket_entries(); i++)
				hash_table.insert(std::to_string(i*hash_table.directory_size()), " 1");
			CPPUNIT_ASSERT(hash_table.get_global_depth() == 2);

			// Hitting new bucket doesn't split
			hash_table.insert(std::to_string(hash_table.get_bucket_entries()*hash_table.directory_size()-1), " 1");
			CPPUNIT_ASSERT(hash_table.get_global_depth() == 2);

			// Hitting same bucket DOES split
			hash_table.insert(std::to_string(hash_table.get_bucket_entries()*hash_table.directory_size()), " 1");
			CPPUNIT_ASSERT(hash_table.get_global_depth() == 3);

			// Overflowing another bucket doesn't global split
			for (std::uint32_t i = 0; i < hash_table.get_bucket_entries(); i++)
				hash_table.insert(std::to_string(i*hash_table.directory_size()+1), " 1");
			CPPUNIT_ASSERT(hash_table.get_global_depth() == 3);

			// Refilling originally split buckets doesn't global split
			for (std::uint32_t i = 1; i < hash_table.get_bucket_entries(); i++)
				hash_table.insert(std::to_string((hash_table.get_bucket_entries() + i)*hash_table.directory_size()>>1), " 1");
			CPPUNIT_ASSERT(hash_table.get_global_depth() == 3);

			// Overflowing bucket DOES split
			hash_table.insert(std::to_string(2*hash_table.get_bucket_entries()*hash_table.directory_size()), " 1");
			CPPUNIT_ASSERT(hash_table.get_global_depth() == 4);
		}

		void test_double_split() {
			std::cout << "TEST_DOUBLE_SPLIT" << std::endl;
			CPPUNIT_ASSERT(hash_table.get_global_depth() == 2);

			// Filling one bucket doesn't split
			for (std::uint32_t i = 0; i <= hash_table.get_bucket_entries(); i++)
				hash_table.insert(std::to_string(i*hash_table.directory_size()<<1), " 1");
			CPPUNIT_ASSERT(hash_table.get_global_depth() == 4);			
		}

		void test_insert_delete_many() {
			std::cout << "TEST_INSERT_DELETE_MANY" << std::endl;
			CPPUNIT_ASSERT(hash_table.get_global_depth() == 2);
			std::uint64_t i = 0;
			std::uint8_t  p = 10;
			for (std::uint8_t j = 1; j <= p; j++) {
					for (; i < (std::uint32_t)(1<<j)*hash_table.get_bucket_entries(); i++)
					{
					hash_table.insert(std::to_string(i*hash_table.get_bucket_entries()), " 1");
					}
				CPPUNIT_ASSERT(hash_table.get_global_depth() == 2+j);
				}
			CPPUNIT_ASSERT(hash_table.size() == (std::uint32_t)(1<<p)*hash_table.get_bucket_entries());
			for (i = 0; i < (std::uint32_t)(1<<p)*hash_table.get_bucket_entries(); i++)
				{
				hash_table.remove(std::to_string(i*hash_table.get_bucket_entries()));
				}
			CPPUNIT_ASSERT(hash_table.size() == 0);
		}

		static CppUnit::Test *suite()
		{
			CppUnit::TestSuite *suite_of_tests = new CppUnit::TestSuite( "extendible_hash_table_suite" );
			suite_of_tests->addTest( new CppUnit::TestCaller<extendible_hash_table_test>(
            	           "test_insert",
            	           	&extendible_hash_table_test::test_insert ) );
			suite_of_tests->addTest( new CppUnit::TestCaller<extendible_hash_table_test>(
                	       "test_delete",
                    	   &extendible_hash_table_test::test_delete ) );
			suite_of_tests->addTest( new CppUnit::TestCaller<extendible_hash_table_test>(
                	       "test_update",
                    	   &extendible_hash_table_test::test_update ) );

			suite_of_tests->addTest( new CppUnit::TestCaller<extendible_hash_table_test>(
                	       "test_scan",
                    	   &extendible_hash_table_test::test_scan ) );

			suite_of_tests->addTest( new CppUnit::TestCaller<extendible_hash_table_test>(
                       		"test_split",
                       		&extendible_hash_table_test::test_split ) );
			suite_of_tests->addTest( new CppUnit::TestCaller<extendible_hash_table_test>(
                       		"test_double_split",
                       		&extendible_hash_table_test::test_double_split ) );

			suite_of_tests->addTest( new CppUnit::TestCaller<extendible_hash_table_test>(
                       		"test_insert_delete_many",
                       		&extendible_hash_table_test::test_insert_delete_many ) );
			
			suite_of_tests->addTest( new CppUnit::TestCaller<extendible_hash_table_test>(
                       		"test_concurrent_different",
                       		&extendible_hash_table_test::test_concurrent_different ) );
			suite_of_tests->addTest( new CppUnit::TestCaller<extendible_hash_table_test>(
                       		"test_concurrent_all",
                       		&extendible_hash_table_test::test_concurrent_all ) );
			suite_of_tests->addTest( new CppUnit::TestCaller<extendible_hash_table_test>(
                       		"test_concurrent_updates_known",
                       		&extendible_hash_table_test::test_concurrent_updates_known ) );
			suite_of_tests->addTest( new CppUnit::TestCaller<extendible_hash_table_test>(
                       		"test_concurrent_scans",
                       		&extendible_hash_table_test::test_concurrent_scans ) );
			return suite_of_tests;
		};
	};
}
#endif /* TEST_EXTENDIBLE_HASH_TABLE_TEST_H_ */
