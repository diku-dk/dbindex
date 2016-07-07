#include <string>
#include <cmath>
#include <chrono>
#include <thread>
#include <fstream>
#include <iostream>

#include "../src/hash_functions/abstract_hash.h"
#include "../src/hash_functions/mod_hash.h"
#include "../src/hash_functions/mult_shift_hash.h"
#include "../src/hash_functions/tabulation_hash.h"
#include "../src/hash_functions/murmur_hash_32.h"
#include "../src/hash_index/array_hash_table.h"
#include "../src/hash_index/extendible_hash_table.h"
#include "../src/hash_index/partitioned_array_hash_table.h"
#include "../src/benchmarks/ycsb/client.h"
#include "../src/benchmarks/ycsb/core_workloads.h"
#include "../src/util/memory_util.h"

typedef std::uint32_t hash_value_t;
void calc_performance_results(std::uint32_t iterations, std::uint32_t* throughputs, double& mean, double& std_dev) {
	// Calculating the performance
	std::uint32_t thread_total_throughput = 0;
	for(std::uint32_t i = 0; i < iterations; i++) 
		thread_total_throughput += throughputs[i];
	mean = thread_total_throughput / iterations;
	double var = 0.0;
	for(std::uint32_t i = 0; i < iterations; i++)
	{
		var += (throughputs[i]-mean)*(throughputs[i]-mean);
	}
	var /= iterations;
	std_dev = std::sqrt(var);
	std::cout << "Avg Throughput: " << mean << " operations/second." << std::endl;
	std::cout << "Std Deviation:  " << std_dev << " operations/second." << std::endl;
}

std::string calc_hash_func_string(std::uint32_t hash_func_num) {
	switch (hash_func_num) {
	case 0:
		return "mod_hash";
	case 1:
		return "murmur_hash";
	case 2:
	default:
		return "tabulation_hash";
	}	
}
std::string calc_hash_index_string(std::uint32_t hash_index_num) {
	switch (hash_index_num) {
	case 1:
		return "extendible_hash_table";
	case 2:
		return "partitioned_array_hash_table";
	case 0:
	default:
		return "array_hash_table";
	}	
}

template <std::uint32_t mod_value, std::uint8_t max_key_len_val>
dbindex::abstract_hash<hash_value_t>* calc_hash_func(std::string& hash_func_string) {
	dbindex::abstract_hash<hash_value_t>* hash;
	if (hash_func_string == "mod_hash")
		hash = new dbindex::mod_hash<hash_value_t, mod_value>();
	else if (hash_func_string == "murmur_hash")
		hash = new dbindex::murmur_hash_32<hash_value_t>();
	else if (hash_func_string == "tabulation_hash")
		hash = new dbindex::tabulation_hash<hash_value_t, max_key_len_val>();
	else 
		throw std::invalid_argument("unknown hash_function");
	return hash;
}

template <std::uint8_t directory_depth, std::uint8_t prefix_bits>
dbindex::abstract_index* calc_hash_index(dbindex::abstract_hash<hash_value_t>* hash, std::string& hash_index_string) {
	if (hash_index_string == "array_hash_table")
		return new dbindex::array_hash_table<(1<<directory_depth)>(*hash); 
	else if (hash_index_string == "extendible_hash_table")
		return new dbindex::extendible_hash_table<directory_depth>(*hash);
	else if (hash_index_string == "partitioned_array_hash_table")
		return new dbindex::partitioned_array_hash_table<prefix_bits, (1<<directory_depth)>(*hash);
	else 
		throw std::invalid_argument("unknown hash_index");
	return nullptr;
}

ycsb::workload_properties parse_workload_string(std::string workload_string) {
	if (workload_string.compare("workload_a") == 0)
		return ycsb::workload_a;
	if (workload_string.compare("workload_b") == 0)
		return ycsb::workload_b;
	if (workload_string.compare("workload_c") == 0)
		return ycsb::workload_c;
	if (workload_string.compare("workload_d") == 0)
		return ycsb::workload_d;
	if (workload_string.compare("workload_e") == 0)
		return ycsb::workload_e;
	if (workload_string.compare("workload_f") == 0)
		return ycsb::workload_f;

	if (workload_string.compare("workload_read") == 0)
		return ycsb::workload_read;
	if (workload_string.compare("workload_update") == 0)
		return ycsb::workload_update;
	if (workload_string.compare("workload_scan") == 0)
		return ycsb::workload_scan;
	if (workload_string.compare("workload_insert") == 0)
		return ycsb::workload_insert;
	if (workload_string.compare("workload_rmf") == 0)
		return ycsb::workload_read_modify_write;

	std::cout << "Unknown workload_type, returning workload_a" << std::endl;
	return ycsb::workload_a;
}

std::string workload_to_string(ycsb::workload_properties workload_other) {
	if (workload_other == ycsb::workload_a)
		return "workload_a";
	if (workload_other == ycsb::workload_b)
		return "workload_b";
	if (workload_other == ycsb::workload_c)
		return "workload_c";
	if (workload_other == ycsb::workload_d)
		return "workload_d";
	if (workload_other == ycsb::workload_e)
		return "workload_e";
	if (workload_other == ycsb::workload_f)
		return "workload_f";

	if (workload_other == ycsb::workload_read)
		return "workload_read";
	if (workload_other == ycsb::workload_update)
		return "workload_update";
	if (workload_other == ycsb::workload_scan)
		return "workload_scan";
	if (workload_other == ycsb::workload_insert)
		return "workload_insert";
	if (workload_other == ycsb::workload_read_modify_write)
		return "workload_rmf";

	std::cout << "Unknown workload, returning workload_a" << std::endl;
	return "workload_a";
}

void test_workload(std::uint8_t thread_count_max, std::string workload_string, std::string hash_func_string, std::string hash_index_string, std::string file_suffix) {
	using namespace std::chrono;

	constexpr std::uint8_t  directory_depth = 17;
	constexpr std::uint8_t  prefix_bits     = 4;
	constexpr std::uint32_t max_key_len_val = 14;
	constexpr std::uint32_t mod_value       = 1<<31;
	ycsb::workload_properties workload = parse_workload_string(workload_string);

	std::uint32_t iterations = 5;

	std::uint32_t throughputs[iterations*thread_count_max];

	dbindex::abstract_hash<hash_value_t>* hash = calc_hash_func<mod_value, max_key_len_val>(hash_func_string);
	dbindex::abstract_index* hash_index        = calc_hash_index<directory_depth, prefix_bits>(hash, hash_index_string);

	ycsb::client client(*hash_index, workload);
	client.run_build_records(1);

	if (workload_string == "workload_hash_func") {
		auto ops = 100000;

		std::ofstream out_file;
		std::string path = "../Thesis/results/hash_func_test" + file_suffix + ".txt";
		std::cout << path << std::endl;
		out_file.open (path);
		out_file.clear();
		if (!out_file.is_open()) {
			std::cout << "Data file isn't open." << std::endl;
			return;
		}
		for(std::uint32_t t = 0; t < thread_count_max; t++) {
			std::cout << "Thread count: " << t << ": " << std::endl;
			for(std::uint32_t i = 0; i < iterations; i++) {
				throughputs[t*iterations + i] = client.run_hash_func(*hash, t+1, ops);
				std::cout << "Iteration: " << i << ": " <<  throughputs[t*iterations + i] << std::endl;
			}
			// Calculating the performance
			double mean, std_dev;
			calc_performance_results(iterations, &throughputs[t*iterations], mean, std_dev);

			// Writing data to file
			out_file << (t+1) << "\t" << mean << "\t" << std_dev << "\n";
			std::cout << "Data written" << std::endl;
		}
		return;
	} else if (workload_string == "workload_lock") {
		auto ops = 100000;

		std::ofstream out_file;
		std::string path = "../Thesis/results/lock_test" + file_suffix + ".txt";
		std::cout << path << std::endl;
		out_file.open (path);
		out_file.clear();
		if (!out_file.is_open()) {
			std::cout << "Data file isn't open." << std::endl;
			return;
		}
		for(std::uint32_t t = 0; t < thread_count_max; t++) {
			std::cout << "Thread count: " << t << ": " << std::endl;
			for(std::uint32_t i = 0; i < iterations; i++) {
				throughputs[t*iterations + i] = client.run_locks(t+1, ops);
				std::cout << "Iteration: " << i << ": " <<  throughputs[t*iterations + i] << std::endl;
			}
			// Calculating the performance
			double mean, std_dev;
			calc_performance_results(iterations, &throughputs[t*iterations], mean, std_dev);

			// Writing data to file
			out_file << (t+1) << "\t" << mean << "\t" << std_dev << "\n";
			std::cout << "Data written" << std::endl;
		}
		return;
	} 
	// else if (workload_string == "workload_map") {
	// 	auto ops = 100000;

	// 	std::ofstream out_file;
	// 	std::string path = "../Thesis/results/map_test" + file_suffix + ".txt";
	// 	std::cout << path << std::endl;
	// 	out_file.open (path);
	// 	out_file.clear();
	// 	if (!out_file.is_open()) {
	// 		std::cout << "Data file isn't open." << std::endl;
	// 		return;
	// 	}

	// 	std::map<std::string, std::string> shared_map;

	// 	client.run_build_records_map(shared_map, 1, ops);

	// 	for(std::uint32_t t = 0; t < thread_count_max; t++) {
	// 		std::cout << "Thread count: " << t << ": " << std::endl;
	// 		for(std::uint32_t i = 0; i < iterations; i++) {
	// 			throughputs[t*iterations + i] = client.run_map(shared_map, t+1, ops);
	// 			std::cout << "Iteration: " << i << ": " <<  throughputs[t*iterations + i] << std::endl;
	// 		}
	// 		// Calculating the performance

			// double mean, std_dev;
			// calc_performance_results(iterations, &throughputs[t*iterations], mean, std_dev);
	// 		// Writing data to file
	// 		out_file << (t+1) << "\t" << mean << "\t" << std_dev << "\n";
	// 		std::cout << "Data written" << std::endl;
	// 		std::this_thread::sleep_for(std::chrono::seconds(2));
	// 	}
	// 	return;
	// } 
	// else if (workload_string == "workload_map_locked") {
	// 	auto ops = 100000;

	// 	std::ofstream out_file;
	// 	std::string path = "../Thesis/results/map_locked_test" + file_suffix + ".txt";
	// 	std::cout << path << std::endl;
	// 	out_file.open (path);
	// 	out_file.clear();
	// 	if (!out_file.is_open()) {
	// 		std::cout << "Data file isn't open." << std::endl;
	// 		return;
	// 	}

	// 	std::map<std::string, std::string> shared_map;

	// 	client.run_build_records_map(shared_map, 1, ops);

	// 	for(std::uint32_t t = 0; t < thread_count_max; t++) {
	// 		std::cout << "Thread count: " << t << ": " << std::endl;
	// 		for(std::uint32_t i = 0; i < iterations; i++) 
	// 		{
	// 			throughputs[t*iterations + i] = client.run_map_locked(shared_map, t+1, ops);
	// 			std::cout << "Iteration: " << i << ": " <<  throughputs[t*iterations + i] << std::endl;
	// 		}
	// 		// Calculating the performance

			// double mean, std_dev;
			// calc_performance_results(iterations, &throughputs[t*iterations], mean, std_dev);
	// 		// Writing data to file
	// 		out_file << (t+1) << "\t" << mean << "\t" << std_dev << "\n";
	// 		std::cout << "Data written" << std::endl;
	// 		std::this_thread::sleep_for(std::chrono::seconds(2));
	// 	}
	// 	return;
	// } 

	// Calculating the hashing

	std::cout << "Opening file" << std::endl;
		// Opening Data File
	std::ofstream out_file;
	std::string path = "../Thesis/results/" + hash_func_string + "_" + hash_index_string + "_" + workload_string + file_suffix + ".txt";
	std::cout << path << std::endl;
	out_file.open (path);
	out_file.clear();
	if (!out_file.is_open()) {
		std::cout << "Data file isn't open." << std::endl;
		return;
	}
	std::cout << "File opened" << std::endl;

		// Running experiments 
	std::cout << "Running benchmark with " << workload_to_string(workload) << std::endl;
	for(std::uint32_t t = 0; t < thread_count_max; t++) {
		std::cout << "Thread count: " << t << ": " << std::endl;

		for(std::uint32_t i = 0; i < iterations; i++) 
		{

			throughputs[t*iterations + i] = client.run_transactions(t+1); //workload.operation_count*1000*(t+1)/client.run_transactions(t+1);
			std::cout << "Iteration: " << i << ": " <<  throughputs[t*iterations + i] << std::endl;
			if (workload.insert_proportion > 0 && i+1 < iterations) {
				delete hash_index;
				delete hash;

				dbindex::abstract_hash<hash_value_t>* hash = calc_hash_func<mod_value, max_key_len_val>(hash_func_string);
				dbindex::abstract_index* hash_index        = calc_hash_index<directory_depth, prefix_bits>(hash, hash_index_string);

				ycsb::client client(*hash_index, workload);
				client.run_build_records(1);
			}

		}

		// Calculating the performance
		double mean, std_dev;
		calc_performance_results(iterations, &throughputs[t*iterations], mean, std_dev);

		// Writing data to file
		out_file << (t+1) << "\t" << mean << "\t" << std_dev << "\t" << utils::get_peak_RSS() << "\t" << utils::get_current_RSS() << "\n";
		out_file.flush();
		std::cout << "Data written" << std::endl;
		std::this_thread::sleep_for(std::chrono::seconds(2));
	}
	delete hash_index;
	delete hash;

	if (out_file.fail())
	  std::cout << "Something failed" << std::endl;
	out_file.close();
}

template <std::uint8_t directory_depth>
void test_dir_size(std::uint8_t thread_count, ycsb::workload_properties workload, std::string hash_func_string, std::string hash_index_string, std::string file_suffix, std::ofstream& out_file) {
	static_assert(directory_depth < 32, "Too large directory_depth");
	using namespace std::chrono;

	constexpr std::uint32_t directory_size = 1<<directory_depth;
	constexpr std::uint8_t prefix_bits = 4;
	constexpr std::uint32_t mod_value = 1<<31;
	constexpr std::uint32_t max_key_len_val = 14;
	std::cout << "Using directory_depth = " << (int)directory_depth << std::endl;

		// Running experiments 
	std::uint32_t iterations = 25;
	std::uint32_t throughputs[iterations]; 
	std::cout << "Running benchmark with " << workload_to_string(workload) << std::endl;

	// Initial setup
	dbindex::abstract_hash<hash_value_t>* hash = calc_hash_func<mod_value, max_key_len_val>(hash_func_string);
	dbindex::abstract_index* hash_index = calc_hash_index<directory_depth, prefix_bits>(hash, hash_index_string);
		// Building client, inserting records.
	ycsb::client client(*hash_index, workload);
	client.run_build_records(1);

	for(std::uint32_t i = 0; i < iterations; i++) 
	{

		throughputs[i] = client.run_transactions(thread_count);
		std::cout << "Iteration: " << i << ": " <<  throughputs[i] << std::endl;	

		// Fixing if new inserts were done
		if (workload.insert_proportion > 0 && i+1 < iterations) {
			delete hash_index;
			delete hash;

			dbindex::abstract_hash<hash_value_t>* hash = calc_hash_func<mod_value, max_key_len_val>(hash_func_string);
			dbindex::abstract_index* hash_index = calc_hash_index<directory_depth, prefix_bits>(hash, hash_index_string);
				// Building client, inserting records.
			ycsb::client client(*hash_index, workload);
			client.run_build_records(1);
		}
		std::this_thread::sleep_for(std::chrono::seconds(1));
	}
	
	delete hash_index;
	delete hash;
	double mean, std_dev;
	calc_performance_results(iterations, &throughputs[0], mean, std_dev);

	// Writing data to file
	out_file << directory_size << "\t" << mean << "\t" << std_dev << "\t" << utils::get_peak_RSS() << "\t" << utils::get_current_RSS() << "\n";
	std::cout << "Data written" << std::endl;
	out_file.flush();

	test_dir_size<directory_depth-1>(thread_count, workload, hash_func_string, hash_index_string, file_suffix, out_file);
}
template <>
void test_dir_size<0>(std::uint8_t thread_count, ycsb::workload_properties workload, std::string hash_func_string, std::string hash_index_string, std::string file_suffix, std::ofstream& out_file) {
	// Ending the recursion
}
template <std::int8_t initial_global_depth>
void test_initial_global_depth(std::uint8_t thread_count, ycsb::workload_properties workload, std::string hash_func_string, std::string file_suffix, std::ofstream& out_file) {
	static_assert(initial_global_depth < 32, "Too large global_depth");
	using namespace std::chrono;

	constexpr std::uint32_t mod_value = 1<<31;
	constexpr std::uint32_t max_key_len_val = 14;
	std::cout << "Using initial_global_depth = " << (int)initial_global_depth << std::endl;

		// Running experiments 
	std::uint32_t iterations = 25;
	std::uint32_t throughputs[iterations]; 

	std::cout << "Running benchmark with " << workload_to_string(workload) << std::endl;

	// Initial setup
	dbindex::abstract_hash<hash_value_t>* hash = calc_hash_func<mod_value, max_key_len_val>(hash_func_string);
	dbindex::abstract_index* hash_index = new dbindex::extendible_hash_table<initial_global_depth>(*hash);
		// Building client, inserting records.
	ycsb::client client(*hash_index, workload);
	client.run_build_records(1);

	for(std::uint32_t i = 0; i < iterations; i++) 
	{

		throughputs[i] = client.run_transactions(thread_count);
		std::cout << "Iteration: " << i << ": " <<  throughputs[i] << std::endl;

		// Fixing if new inserts were done
		if (workload.insert_proportion > 0 && i+1 < iterations) {
			delete hash_index;
			delete hash;

			dbindex::abstract_hash<hash_value_t>* hash = calc_hash_func<mod_value, max_key_len_val>(hash_func_string);
			dbindex::abstract_index* hash_index = new dbindex::extendible_hash_table<initial_global_depth>(*hash);
				// Building client, inserting records.
			ycsb::client client(*hash_index, workload);
			client.run_build_records(1);
		}
		std::this_thread::sleep_for(std::chrono::seconds(1));

	}
	delete hash_index;
	delete hash;

	double mean, std_dev;
	calc_performance_results(iterations, &throughputs[0], mean, std_dev);

	// Writing data to file
	out_file << (1<<initial_global_depth) << "\t" << mean << "\t" << std_dev << "\t" << utils::get_peak_RSS() << "\t" << utils::get_current_RSS() << "\n";
	std::cout << "Data written" << std::endl;
	out_file.flush();
	
	test_initial_global_depth<initial_global_depth-1>(thread_count, workload, hash_func_string, file_suffix, out_file);
}
template <>
void test_initial_global_depth<0>(std::uint8_t thread_count, ycsb::workload_properties workload, std::string hash_func_string, std::string file_suffix, std::ofstream& out_file) {
	// Ending the recursion
}

template <std::uint8_t prefix_bits>
void test_prefix_bits(std::uint8_t thread_count, ycsb::workload_properties workload, std::string hash_func_string, std::string file_suffix, std::ofstream& out_file) {
	using namespace std::chrono;

	constexpr std::uint32_t directory_size = 1<<17;
	constexpr std::uint32_t mod_value = 1<<31;
	constexpr std::uint32_t max_key_len_val = 14;
	std::cout << "Using prefix_bits = " << (int)prefix_bits << std::endl;


		// Running experiments 
	std::uint32_t iterations = 25;
	std::uint32_t throughputs[iterations]; 

	std::cout << "Running benchmark with " << workload_to_string(workload) << std::endl;

	// Initial setup
	dbindex::abstract_hash<hash_value_t>* hash = calc_hash_func<mod_value, max_key_len_val>(hash_func_string);
	dbindex::abstract_index* hash_index = new dbindex::partitioned_array_hash_table<prefix_bits, directory_size>(*hash);
		// Building client, inserting records.
	ycsb::client client(*hash_index, workload);
	client.run_build_records(1);

	for(std::uint32_t i = 0; i < iterations; i++) 
	{

		throughputs[i] = client.run_transactions(thread_count);
		std::cout << "Iteration: " << i << ": " <<  throughputs[i] << std::endl;

		// Fixing if new inserts were done
		if (workload.insert_proportion > 0 && i+1 < iterations) {
			delete hash_index;
			delete hash;

			dbindex::abstract_hash<hash_value_t>* hash = calc_hash_func<mod_value, max_key_len_val>(hash_func_string);
			dbindex::abstract_index* hash_index = new dbindex::partitioned_array_hash_table<prefix_bits, directory_size>(*hash);
				// Building client, inserting records.
			ycsb::client client(*hash_index, workload);
			client.run_build_records(1);
		}
		std::this_thread::sleep_for(std::chrono::seconds(1));
	}

	delete hash_index;
	delete hash;
	double mean, std_dev;
	calc_performance_results(iterations, &throughputs[0], mean, std_dev);

	// Writing data to file
	out_file << prefix_bits << "\t" << mean << "\t" << std_dev << "\t" << utils::get_peak_RSS() << "\t" << utils::get_current_RSS() << "\n";
	std::cout << "Data written" << std::endl;
	out_file.flush();
	
	test_prefix_bits<prefix_bits-1>(thread_count, workload, hash_func_string, file_suffix, out_file);
}
template <>
void test_prefix_bits<0>(std::uint8_t thread_count, ycsb::workload_properties workload, std::string hash_func_string, std::string file_suffix, std::ofstream& out_file) {
	// Ending the recursion
}

// template <std::uint8_t max_key_len>
// void test_max_key_len(std::uint8_t thread_count, ycsb::workload_properties workload, std::string hash_func_string, std::string hash_index_string, std::string file_suffix, std::ofstream& out_file) {
// 	using namespace std::chrono;

// 	constexpr std::uint32_t directory_size = 1<<17;
// 	constexpr std::uint8_t prefix_bits = 4;
// 	constexpr std::uint32_t mod_value = 1<<31;

// 	dbindex::abstract_hash<hash_value_t>* hash = calc_hash_func<mod_value, max_key_len>(hash_func_string);
// 	dbindex::abstract_index* hash_index = calc_hash_index<directory_depth, prefix_bits>(hash, hash_index_string);
// 	std::cout << "Using max_key_len = " << (int)max_key_len << std::endl;
// 		// Building client, inserting records.
// 	ycsb::client client(*hash_index, workload);
// 	client.run_build_records(1);
// 	std::cout << "Records built" << std::endl;

// 		// Running experiments 
// 	std::uint32_t iterations = 25;
// 	std::uint32_t throughputs[iterations]; 

// 	std::cout << "Running benchmark with " << workload_to_string(workload) << std::endl;
// 	for(std::uint32_t i = 0; i < iterations; i++) 
// 	{
// 		throughputs[i] = client.run_transactions(thread_count);
// 		std::cout << "Iteration: " << i << ": " <<  throughputs[i] << std::endl;
// 	}

// 	double mean, std_dev;
// 	calc_performance_results(iterations, &throughputs[0], mean, std_dev);

// 	// Writing data to file
// 	out_file << directory_size << "\t" << mean << "\t" << std_dev << "\t" << utils::get_peak_RSS() << "\t" << utils::get_current_RSS() << "\n";
// 	std::cout << "Data written" << std::endl;
// 	out_file.flush();
	
	
// 	delete hash_index;
// 	delete hash;
// 	test_max_key_len<directory_depth-1>(thread_count, workload, hash_func_string, hash_index_string, file_suffix, out_file);
// }
// template <>
// void test_max_key_len<0>(std::uint8_t thread_count, ycsb::workload_properties workload, std::string hash_func_string, std::string hash_index_string, std::string file_suffix, std::ofstream& out_file) {
// 	// Ending the recursion
// }

int main(int argc, char *argv[]) {
	std::string workload_string   = "workload_a";
	std::uint8_t hash_func_num    = 2;
	std::uint8_t hash_index_num   = 0;
	std::uint8_t thread_count_max = 16;
	std::string file_suffix       = "";

	if (argc < 5) {
		std::cout << "You must provide the following arguments:\n\t1. Workload\n\t2. Hash function\n\t3. Hash index\n\t4. Thread count\n" << std::endl;
		std::cout << "Valid workloads (string):" << std::endl;
		std::cout << "\t- workload_a      =  50% Read, 50% Update" << std::endl;
		std::cout << "\t- workload_b      =  95% Read,  5% Update w/ ZIPFIAN distribution" << std::endl;
		std::cout << "\t- workload_c      = 100% Read" << std::endl;
		std::cout << "\t- workload_d      =  95% Read,  5% Update w/ LATEST distribution" << std::endl;
		std::cout << "\t- workload_e      =  95% Scan,  5% Inserts " << std::endl;
		std::cout << "\t- workload_f      =  50% Read, 50% Read-Modify-Write" << std::endl;
		std::cout << std::endl;
		std::cout << "\t- workload_read   = 100% Read" << std::endl;
		std::cout << "\t- workload_update = 100% Update" << std::endl;
		std::cout << "\t- workload_scan   = 100% Scan" << std::endl;
		std::cout << "\t- workload_insert = 100% Insert" << std::endl;
		std::cout << "\t- workload_rmf    = 100% Read-Modify-Write" << std::endl;
		std::cout << std::endl;
		std::cout << "Hash function options (integer):" << std::endl;
		std::cout << "\t- 0 = Mod" << std::endl;
		std::cout << "\t- 1 = Murmur" << std::endl;
		std::cout << "\t- 2 = Tabulation" << std::endl;
		std::cout << std::endl;
		std::cout << "Hash index options (integer):" << std::endl;
		std::cout << "\t- 0 = Array" << std::endl;
		std::cout << "\t- 1 = Extendible" << std::endl;
		std::cout << "\t- 2 = Partitioned Array" << std::endl;
		return 0;
	}
	workload_string  = argv[1];
	hash_func_num    = (std::uint8_t)(argv[2][0]-'0');
	hash_index_num   = (std::uint8_t)(argv[3][0]-'0');
	thread_count_max = std::atoi(argv[4]);

	std::string hash_func_string  = calc_hash_func_string(hash_func_num);
	std::string hash_index_string = calc_hash_index_string(hash_index_num);
	std::cout << "Using " << hash_func_string << " and " << hash_index_string << std::endl;

	if (argc > 5)
		file_suffix = argv[5];
	
	std::string dir_size_string = "_dir_size";
	std::string max_key_len_string = "_max_key_len";
	std::string global_depth_string = "_global_depth";
	std::string prefix_bits_string = "_prefix_bits";

	if (workload_string.substr(workload_string.length()-dir_size_string.length())==dir_size_string) {
		assert(hash_index_num == 0 || hash_index_num == 2);
		constexpr std::uint8_t max_directory_depth = 20;
		workload_string = workload_string.substr(0, workload_string.length()-dir_size_string.length());
		ycsb::workload_properties workload = parse_workload_string(workload_string);

		std::ofstream out_file;
		std::string path = "../Thesis/results/" + hash_func_string + "_" + hash_index_string + "_" + workload_string + dir_size_string + file_suffix + ".txt";
		std::cout << path << std::endl;
		out_file.open (path);
		out_file.clear();
		if (!out_file.is_open()) {
			std::cout << "Data file isn't open." << std::endl;
			return -1;
		}
		std::cout << "File opened" << std::endl;

		test_dir_size<max_directory_depth>(thread_count_max, workload, hash_func_string, hash_index_string, file_suffix, out_file);
	
		out_file.flush();
		if (out_file.fail())
		  std::cout << "Something failed" << std::endl;
		out_file.close();
	} else if (workload_string.substr(workload_string.length()-global_depth_string.length())==global_depth_string) {
		assert(hash_index_num == 1);
		constexpr std::uint8_t max_initial_global_depth = 20;
		workload_string = workload_string.substr(0, workload_string.length()-global_depth_string.length());
		ycsb::workload_properties workload = parse_workload_string(workload_string);

		std::ofstream out_file;
		std::string path = "../Thesis/results/" + hash_func_string + "_" + hash_index_string + "_" + workload_string + global_depth_string + file_suffix + ".txt";
		std::cout << path << std::endl;
		out_file.open (path);
		out_file.clear();
		if (!out_file.is_open()) {
			std::cout << "Data file isn't open." << std::endl;
			return -1;
		}
		std::cout << "File opened" << std::endl;

		test_initial_global_depth<max_initial_global_depth>(thread_count_max, workload, hash_func_string, file_suffix, out_file);
	
		out_file.flush();
		if (out_file.fail())
		  std::cout << "Something failed" << std::endl;
		out_file.close();
	} else if (workload_string.substr(workload_string.length()-prefix_bits_string.length())==prefix_bits_string) {
		assert(hash_index_num == 2);
		constexpr std::uint8_t max_prefix_bits = 31;
		workload_string = workload_string.substr(0, workload_string.length()-prefix_bits_string.length());
		ycsb::workload_properties workload = parse_workload_string(workload_string);

		std::ofstream out_file;
		std::string path = "../Thesis/results/" + hash_func_string + "_" + hash_index_string + "_" + workload_string + prefix_bits_string + file_suffix + ".txt";
		std::cout << path << std::endl;
		out_file.open (path);
		out_file.clear();
		if (!out_file.is_open()) {
			std::cout << "Data file isn't open." << std::endl;
			return -1;
		}
		std::cout << "File opened" << std::endl;

		test_prefix_bits<max_prefix_bits>(thread_count_max, workload, hash_func_string, file_suffix, out_file);
	
		out_file.flush();
		if (out_file.fail())
		  std::cout << "Something failed" << std::endl;
		out_file.close();
	// } else if (workload_string.substr(workload_string.length()-max_key_len_string.length())==max_key_len_string) {
	// 	constexpr std::uint8_t max_key_len = 64;
	// 	workload_string = workload_string.substr(0, workload_string.length()-max_key_len_string.length());
	// 	ycsb::workload_properties workload = parse_workload_string(workload_string);

	// 	std::ofstream out_file;
	// 	std::string path = "../Thesis/results/" + hash_func_string + "_" + hash_index_string + "_" + workload_string + max_key_len_string + file_suffix + ".txt";
	// 	std::cout << path << std::endl;
	// 	out_file.open (path);
	// 	out_file.clear();
	// 	if (!out_file.is_open()) {
	// 		std::cout << "Data file isn't open." << std::endl;
	// 		return -1;
	// 	}
	// 	std::cout << "File opened" << std::endl;

	// 	test_max_key_len<max_directory_depth>(thread_count_max, workload, hash_func_string, hash_index_string, file_suffix, out_file);
	
	// 	out_file.flush();
	// 	if (out_file.fail())
	// 	  std::cout << "Something failed" << std::endl;
	// 	out_file.close();
	} else {
		test_workload(thread_count_max, workload_string, hash_func_string, hash_index_string, file_suffix);
	}
}
