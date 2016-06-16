#include "extendible_hash_table_test.h"
#include "array_hash_table_test.h"
#include "partitioned_array_hash_table_test.h"
#include <cppunit/TestCase.h>
#include <cppunit/TestFixture.h>
#include <cppunit/ui/text/TestRunner.h>
#include <cppunit/TestResult.h>
#include <cppunit/TestRunner.h>
#include <iostream>
#include <iomanip>

#include "../src/benchmarks/ycsb/client.h"
#include "../src/benchmarks/ycsb/workload.h"
#include "../src/benchmarks/ycsb/core_workloads.h"

int main(int argc, char *argv[]) {
	CppUnit::TextUi::TestRunner runner;
	runner.addTest( dbindex::extendible_hash_table_test::suite() );
	runner.addTest( dbindex::array_hash_table_test::suite() );
	runner.addTest( dbindex::partitioned_array_hash_table_test::suite() );

	runner.run();
	std::cout << "end" << std::endl;
}