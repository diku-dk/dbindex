#ifndef extendible_hash_table_test_h
#define extendible_hash_table_test_h

#include <cppunit/TestFixture.h>
#include <cppunit/TestAssert.h>
#include <cppunit/TestCaller.h>
#include <cppunit/TestSuite.h>
#include "../src/abstract_index.h"
#include "../src/hash_index/extendible_hash_table.h"
#include "../src/hash_functions/mod_hash.h"
#include <boost/thread.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <thread>
#include <pthread.h>

#define hash_value_t std::uint32_t
typedef boost::shared_mutex shared_mutex;

namespace dbindex {
	int stick_thread_to_core(pthread_t thread, int core_id) {
	   	cpu_set_t cpuset;
   		CPU_ZERO(&cpuset);
   		CPU_SET(core_id, &cpuset);
		
		return pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
	}
	constexpr std::uint8_t initial_global_depht = 2;

	void insert_concurrent(extendible_hash_table<std::string, std::string, initial_global_depht> *hash_table, std::string *keys, std::uint32_t amount) {
		// Calculating the hashing
		for(std::uint32_t j = 0; j < amount; j++) {
			hash_table->insert(keys[j], keys[j]);
			// std::cout << keys[j] << " DONE" << std::endl;
		}
	}

	shared_mutex mutex;

	void test_upgrade()
	{
		std::cout << "Start: " << boost::this_thread::get_id() << std::endl;
		boost::shared_lock<shared_mutex> read_lock(mutex);
		std::cout << "Shared: " << boost::this_thread::get_id() << std::endl;
		sleep(2);
		std::cout << "Slept1: " << boost::this_thread::get_id() << std::endl;
		std::cout << "Slept1: " << boost::this_thread::get_id() << std::endl;
		std::cout << "Slept1: " << boost::this_thread::get_id() << std::endl;
		std::cout << "Slept1: " << boost::this_thread::get_id() << std::endl;
		std::cout << "Slept1: " << boost::this_thread::get_id() << std::endl;
		boost::upgrade_lock<shared_mutex> upgrade_lock(mutex);
		boost::upgrade_to_unique_lock<shared_mutex> write_lock(upgrade_lock);
		std::cout << "Upgraded: " << boost::this_thread::get_id() << std::endl;
		sleep(10);
		std::cout << "Slept2: " << boost::this_thread::get_id() << std::endl;
		// boost::upgrade_to_unique_lock<shared_mutex> write_lock(read_lock);
		sleep(3);
		std::cout << "Slept3: " << boost::this_thread::get_id() << std::endl;
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

	class extendible_hash_table_test : public CppUnit::TestFixture {
	private:
		abstract_hash<std::uint32_t> *hash;
		extendible_hash_table<std::string, std::string, initial_global_depht> *hash_table;
		abstract_push_op *concat_push;

	public:
		extendible_hash_table_test(){}

		void setUp() {
			hash = new mod_hash<hash_value_t, (1<<31)>();
			hash_table = new extendible_hash_table<std::string, std::string, initial_global_depht>(hash);
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
		}

		void test_insert_delete_many() {
			std::cout << "TEST_INSERT_DELETE_MANY" << std::endl;
			CPPUNIT_ASSERT(hash_table->get_global_depth() == 2);
			std::uint64_t i = 0;
			std::uint8_t  p = 10;
			for (std::uint8_t j = 1; j <= p; j++) {
	  			for (; i < (1<<j)*hash_table->get_bucket_entries(); i++)
	  			{
					hash_table->insert(std::to_string(i*hash_table->get_bucket_entries()), " 1");
	  			}
				CPPUNIT_ASSERT(hash_table->get_global_depth() == 2+j);
	  		}
			CPPUNIT_ASSERT(hash_table->size() == (1<<p)*hash_table->get_bucket_entries());
			for (i = 0; i < (1<<p)*hash_table->get_bucket_entries(); i++)
  			{
				hash_table->remove(std::to_string(i*hash_table->get_bucket_entries()));
  			}
			CPPUNIT_ASSERT(hash_table->size() == 0);
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

		void test_split() {
			std::cout << "TEST_SPLIT" << std::endl;
			// No split from start
			CPPUNIT_ASSERT(hash_table->get_global_depth() == 2);

			// Filling one bucket doesn't split
			for (std::uint32_t i = 0; i < hash_table->get_bucket_entries(); i++)
				hash_table->insert(std::to_string(i*hash_table->directory_size()), " 1");
			CPPUNIT_ASSERT(hash_table->get_global_depth() == 2);

			// Hitting new bucket doesn't split
			hash_table->insert(std::to_string(hash_table->get_bucket_entries()*hash_table->directory_size()-1), " 1");
			CPPUNIT_ASSERT(hash_table->get_global_depth() == 2);

			// Hitting same bucket DOES split
			hash_table->insert(std::to_string(hash_table->get_bucket_entries()*hash_table->directory_size()), " 1");
			CPPUNIT_ASSERT(hash_table->get_global_depth() == 3);

			// Overflowing another bucket doesn't global split
			for (std::uint32_t i = 0; i < hash_table->get_bucket_entries(); i++)
				hash_table->insert(std::to_string(i*hash_table->directory_size()+1), " 1");
			CPPUNIT_ASSERT(hash_table->get_global_depth() == 3);

			// Refilling originally split buckets doesn't global split
			for (std::uint32_t i = 1; i < hash_table->get_bucket_entries(); i++)
				hash_table->insert(std::to_string((hash_table->get_bucket_entries() + i)*hash_table->directory_size()>>1), " 1");
			CPPUNIT_ASSERT(hash_table->get_global_depth() == 3);

			// Overflowing bucket DOES split
			hash_table->insert(std::to_string(2*hash_table->get_bucket_entries()*hash_table->directory_size()), " 1");
			CPPUNIT_ASSERT(hash_table->get_global_depth() == 4);
		}

		void test_double_split() {
			std::cout << "TEST_DOUBLE_SPLIT" << std::endl;
			CPPUNIT_ASSERT(hash_table->get_global_depth() == 2);

			// Filling one bucket doesn't split
			for (std::uint32_t i = 0; i <= hash_table->get_bucket_entries(); i++)
				hash_table->insert(std::to_string(i*hash_table->directory_size()<<1), " 1");
			CPPUNIT_ASSERT(hash_table->get_global_depth() == 4);			
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
				threads[t] = std::thread(insert_concurrent, hash_table, &keys[t*amount], amount);
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
				threads[t] = std::thread(insert_concurrent, hash_table, &keys[t*amount], amount);
				stick_thread_to_core(threads[t].native_handle(), (t*2)+1);
			}
			for(std::uint32_t t = 0; t < num_threads; t++) {
				threads[t].join();
			}
			CPPUNIT_ASSERT(hash_table->size() == num_threads*amount);
			delete[] keys;
		}

		void test_spec() {
			// std::uint8_t  num_threads = 2;
			// boost::thread   threads[num_threads];

 		// 	for(std::uint32_t t = 0; t < num_threads; t++) {
			// 	threads[t] = boost::thread(test_upgrade);
			// 	stick_thread_to_core(threads[t].native_handle(), (t*2)+1);
			// }
			// for(std::uint32_t t = 0; t < num_threads; t++) {
			// 	threads[t].join();
			// }
			std::vector<int> spec = {0, 65536, 65537, 131072, 131073, 131074, 131075, 131076, 131077, 131078, 131079, 131080, 65538, 65539, 65540, 196608, 1, 2, 3, 4, 131081, 131082, 131083, 131084, 65541, 65542, 65543, 65544, 65545, 131085, 131086, 131087, 131088, 65546, 65547, 65548, 65549, 65550, 65551, 65552, 196609, 196610, 196611, 5, 196612, 131089, 196613};
			// {(1<<8)+1, (1<<9)+1, (1<<10)+1, (1<<11)+1, (1<<12)+1, 1, 5, 9, 13};
			// {0, 1, 2, 3, 4, 5, 320, 321, 6, 640, 641, 642, 643, 960, 961, 962, 963, 964, 644, 645, 646, 647, 648, 648, 648, 648, 649, 649, 649, 649, 650, 651, 652, 653, 654, 655, 656, 657, 658, 659, 660, 661, 662, 663, 664, 665, 666, 667, 668, 669, 670, 671, 672, 673, 674, 675, 676, 677, 678, 679, 680, 681, 682, 683, 684, 685, 686, 687, 688, 689, 690, 691, 692, 693, 694, 695, 696, 697, 698, 699, 700, 701, 702, 703, 704, 705, 706, 707, 708, 709, 710, 711, 712, 713, 714, 715, 716, 717, 718, 719, 720, 721, 722, 723, 724, 725, 726, 727, 728, 729, 730, 731, 732, 733, 734, 735, 736, 737, 738, 739, 740, 741, 742, 743, 744, 745, 746, 747, 748, 749, 750, 751, 752, 753, 754, 755, 756, 757, 758, 759, 760, 761, 762, 763, 764, 765, 766, 767, 768, 769, 770, 771, 772, 773, 774, 775, 776, 777, 778, 779, 780, 781, 782, 783, 784, 785, 786, 787, 788, 789, 790, 791, 792, 793, 794, 795, 796, 797, 798, 799, 800, 801, 802, 803, 804, 805, 806, 807, 808, 809, 810, 811, 812, 813, 814, 815, 816, 817, 818, 819, 820, 821, 822, 823, 824, 825, 826, 827, 828, 829, 830, 831, 832, 833, 834, 835, 836, 837, 838, 839, 840, 841, 842, 843, 844, 845, 846, 847, 848, 849, 850, 851, 852, 853, 854, 855, 856, 857, 858, 859, 860, 861, 862, 863, 864, 865, 866, 867, 868, 869, 870, 871, 872, 873, 874, 875, 876, 877, 878, 879, 880, 881, 882, 883, 884, 885, 886, 887, 888, 889, 890, 891, 892, 893, 894, 895, 322, 323, 896, 897, 898, 899, 900, 901, 902, 903, 904, 905, 906, 907, 908, 909, 910, 911, 912, 913, 914, 915, 916, 917, 918, 919, 920, 921, 922, 923, 924, 925, 926, 927, 928, 929, 930, 931, 932, 933, 934, 935, 936, 937, 938, 939, 940, 941, 942, 943, 944, 945, 946, 947, 948, 949, 950, 951, 952, 953, 954, 955, 956, 957, 958, 959, 324, 325, 326, 327, 328, 329, 330, 331, 332, 333, 334, 335, 336, 337, 338, 339, 340, 341, 342, 343, 344, 345, 346, 347, 348, 349, 350, 351, 352, 353, 354, 355, 356, 357, 358, 359, 360, 361, 362, 363, 364, 365, 366, 367, 368, 369, 370, 371, 372, 373, 374, 375, 376, 377, 378, 379, 380, 381, 382, 383, 384, 385, 386, 387, 388, 389, 390, 391, 392, 393, 394, 395, 396, 397, 398, 399, 400, 401, 402, 403, 404, 405, 406, 407, 408, 409, 410, 411, 412, 413, 414, 415, 416, 417, 418, 419, 420, 421, 422, 423, 424, 425, 426, 427, 428, 429, 430, 431, 432, 433, 434, 435, 436, 437, 438, 439, 440, 441, 442, 443, 444, 445, 446, 447, 448, 449, 450, 451, 452, 453, 454, 455, 456, 457, 458, 459, 460, 461, 462, 463, 464, 465, 466, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 965, 966, 967, 968, 969, 970, 971, 972, 973, 974, 975, 976, 977, 978, 979, 980, 981, 982, 983, 984, 985, 986, 987, 988, 989, 990, 991, 992, 993, 994, 995, 996, 997, 998, 999, 1000, 1001, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199, 1002, 1003}; 
			for (auto s : spec) {
				hash_table->insert(std::to_string(s), std::to_string(s));
			}
			// hash_table->print_extendible_hash_table(false);
		}

		bool is_table_empty() {
			return hash_table->size() == 0;
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
                	       "test_scan_reverse",
                    	   &extendible_hash_table_test::test_scan_reverse ) );

			suite_of_tests->addTest( new CppUnit::TestCaller<extendible_hash_table_test>(
                       		"test_split",
                       		&extendible_hash_table_test::test_split ) );
			suite_of_tests->addTest( new CppUnit::TestCaller<extendible_hash_table_test>(
                       		"test_double_split",
                       		&extendible_hash_table_test::test_double_split ) );

			// suite_of_tests->addTest( new CppUnit::TestCaller<extendible_hash_table_test>(
   //                     		"test_insert_delete_many",
   //                     		&extendible_hash_table_test::test_insert_delete_many ) );
			
			// suite_of_tests->addTest( new CppUnit::TestCaller<extendible_hash_table_test>(
   //                     		"test_concurrent_different",
   //                     		&extendible_hash_table_test::test_concurrent_different ) );
			// // for (std::uint32_t i = 0; i < 200; i++) {
			// suite_of_tests->addTest( new CppUnit::TestCaller<extendible_hash_table_test>(
   //                     		"test_concurrent_all",
   //                     		&extendible_hash_table_test::test_concurrent_all ) );
		// }
			// suite_of_tests->addTest( new CppUnit::TestCaller<extendible_hash_table_test>(
   //                     		"test_spec",
   //                     		&extendible_hash_table_test::test_spec ) );
			return suite_of_tests;
		};


		// Implement is_entry_empty
		// Implement size
	};
}
#endif
