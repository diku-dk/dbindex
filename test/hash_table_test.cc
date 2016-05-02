#include "../src/hash_index/extendible_hash_table.h"
#include "extendible_hash_table_test.h"
#include "../src/hash_index/array_hash_table.h"
#include "array_hash_table_test.h"
#include "../src/hash_functions/mod_hash.h"
#include <cppunit/TestCase.h>
#include <cppunit/TestFixture.h>
#include <cppunit/ui/text/TestRunner.h>
#include <cppunit/TestResult.h>
#include <cppunit/TestRunner.h>
#include <iostream>
#include <iomanip>

#define value_t std::uint32_t

	int main(int argc, char *argv[]) {
		CppUnit::TextUi::TestRunner runner;
	  runner.addTest( dbindex::extendible_hash_table_test::suite() );
	  runner.addTest( dbindex::array_hash_table_test::suite() );
	  runner.run();
	}
