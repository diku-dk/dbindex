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
#include "../src/hash_index/partitioned_array_hash_table.h"
#include "../src/hash_index/extendible_hash_table.h"
#include "../src/benchmarks/ycsb/client.h"
#include "../src/benchmarks/ycsb/core_workloads.h"

#define _USE_GLOBAL_LOCKS
#define _USE_LOCAL_LOCKS

typedef std::uint32_t hash_value_t;

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

std::string workload_to_string(ycsb::workload_properties workload_x) {
    if (workload_x == ycsb::workload_a)
        return "workload_a";
    if (workload_x == ycsb::workload_b)
        return "workload_b";
    if (workload_x == ycsb::workload_c)
        return "workload_c";
    if (workload_x == ycsb::workload_d)
        return "workload_d";
    if (workload_x == ycsb::workload_e)
        return "workload_e";
    if (workload_x == ycsb::workload_f)
        return "workload_f";

    if (workload_x == ycsb::workload_read)
        return "workload_read";
    if (workload_x == ycsb::workload_update)
        return "workload_update";
    if (workload_x == ycsb::workload_scan)
        return "workload_scan";
    if (workload_x == ycsb::workload_insert)
        return "workload_insert";
    if (workload_x == ycsb::workload_read_modify_write)
        return "workload_rmf";

    std::cout << "Unknown workload, returning workload_a" << std::endl;
    return "workload_a";
}

void test_workload(std::uint8_t thread_count, std::string workload_string, std::uint8_t hash_func_num, std::uint8_t hash_index_num, std::string file_suffix) {
    using namespace std::chrono;

    constexpr size_t directory_size = 1<<17;
    constexpr size_t initial_global_depht = 17;
    constexpr std::uint8_t prefix_bits = 4;
    constexpr std::uint32_t mod_value = 1<<31;
    constexpr std::uint32_t MAX_KEY_LEN_VAL = 14;

    dbindex::abstract_hash<std::uint32_t>* hash;
    dbindex::abstract_index* hash_table;

    std::string hash_func_string;
    std::string hash_index_string;

    switch (hash_func_num) {
    case 0:
        hash_func_string = "mod_hash";
        hash = new dbindex::mod_hash<hash_value_t, mod_value>();
        break;
    case 1:
        hash_func_string = "murmur_hash";
        hash = new dbindex::murmur_hash_32<hash_value_t>();
        break;
    case 2:
        hash_func_string = "tabulation_hash";
        hash = new dbindex::tabulation_hash<hash_value_t, MAX_KEY_LEN_VAL>();
        break;
    // case 3:
        // hash = new dbindex::mult_shift_hash<hash_value_t> mult();
        // break;
    default:
        std::cout << "Unknown hash_func_num: \"" << hash_func_num << "\"." << std::endl;
        hash_func_string = "tabulation_hash";
        hash = new dbindex::tabulation_hash<hash_value_t, MAX_KEY_LEN_VAL>();
        break;
    }

    switch(hash_index_num) {
    case 0:
        hash_index_string = "array_hash_table";
        hash_table = new dbindex::array_hash_table<directory_size>(*hash); 
        break;
    case 1:
        hash_index_string = "extendible_hash_table";
        hash_table = new dbindex::extendible_hash_table<initial_global_depht>(*hash);
        break;
    case 2:
        hash_index_string = "partitioned_array_hash_table";
        hash_table = new dbindex::partitioned_array_hash_table<prefix_bits, directory_size>(*hash);
        break;
    default:
        std::cout << "Unknown hash_index_num: \"" << hash_index_num << "\"." << std::endl;
        hash_index_string = "extendible_hash_table";
        hash_table = new dbindex::extendible_hash_table<initial_global_depht>(*hash);
        break;
    }
    std::cout << "Using " << hash_func_string << " and " << hash_index_string << std::endl;

    ycsb::workload_properties workload = parse_workload_string(workload_string);

    ycsb::client client(*hash_table, workload, thread_count);

    std::uint32_t iterations = 5;

    std::uint32_t thread_total_throughput;
    std::uint32_t throughputs[iterations*thread_count];

    if (workload_string == "workload_lock") {
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

        for(std::uint32_t t = 0; t < thread_count; t++) {
            std::cout << "Thread count: " << t << ": " << std::endl;
            for(std::uint32_t i = 0; i < iterations; i++) 
            {
                throughputs[t*iterations + i] = workload.operation_count*1000*(t+1)/client.run_locks(t+1, ops);
                std::cout << "Iteration: " << i << ": " <<  throughputs[t*iterations + i] << std::endl;
            }
            // Calculating the performance
            thread_total_throughput = 0;
            for(std::uint32_t i = 0; i < iterations; i++) 
    	        thread_total_throughput += throughputs[t*iterations + i];
            double mean = thread_total_throughput / iterations;
            double var = 0.0;

            for(std::uint32_t i = 0; i < iterations; i++)
            {
                var += (throughputs[t*iterations + i]-mean)*(throughputs[t*iterations + i]-mean);
            }

            var /= iterations;
            double std_dev = std::sqrt(var);
            std::cout << "Avg Throughput: " << mean << " operations/second." << std::endl;
            std::cout << "Std Deviation:  " << std_dev << " operations/second." << std::endl;

            // Writing data to file
            out_file << (t+1) << "\t" << mean << "\t" << std_dev << "\n";
            std::cout << "Data written" << std::endl;
	}
	return;
    } 
    else if (workload_string == "workload_map") {
	auto ops = 100000;

        std::ofstream out_file;
        std::string path = "../Thesis/results/map_test" + file_suffix + ".txt";
        std::cout << path << std::endl;
        out_file.open (path);
        out_file.clear();
        if (!out_file.is_open()) {
            std::cout << "Data file isn't open." << std::endl;
            return;
	}

	std::map<std::string, std::string> shared_map;

	client.run_build_records_map(shared_map, 1, ops);

        for(std::uint32_t t = 0; t < thread_count; t++) {
            std::cout << "Thread count: " << t << ": " << std::endl;
            for(std::uint32_t i = 0; i < iterations; i++) 
            {
                throughputs[t*iterations + i] = client.run_map(shared_map, t+1, ops); // workload.operation_count*1000*(t+1)/client.run_map(shared_map, t+1, ops);
                std::cout << "Iteration: " << i << ": " <<  throughputs[t*iterations + i] << std::endl;
            }
            // Calculating the performance
            thread_total_throughput = 0;
            for(std::uint32_t i = 0; i < iterations; i++) 
    	        thread_total_throughput += throughputs[t*iterations + i];
            double mean = thread_total_throughput / iterations;
            double var = 0.0;

            for(std::uint32_t i = 0; i < iterations; i++)
            {
                var += (throughputs[t*iterations + i]-mean)*(throughputs[t*iterations + i]-mean);
            }

            var /= iterations;
            double std_dev = std::sqrt(var);
            std::cout << "Avg Throughput: " << mean << " operations/second." << std::endl;
            std::cout << "Std Deviation:  " << std_dev << " operations/second." << std::endl;

            // Writing data to file
            out_file << (t+1) << "\t" << mean << "\t" << std_dev << "\n";
            std::cout << "Data written" << std::endl;
	}
	return;
    } 
    else if (workload_string == "workload_map_locked") {
	auto ops = 100000;

        std::ofstream out_file;
        std::string path = "../Thesis/results/map_locked_test" + file_suffix + ".txt";
        std::cout << path << std::endl;
        out_file.open (path);
        out_file.clear();
        if (!out_file.is_open()) {
            std::cout << "Data file isn't open." << std::endl;
            return;
	}

	std::map<std::string, std::string> shared_map;

	client.run_build_records_map(shared_map, 1, ops);

        for(std::uint32_t t = 0; t < thread_count; t++) {
            std::cout << "Thread count: " << t << ": " << std::endl;
            for(std::uint32_t i = 0; i < iterations; i++) 
            {
                throughputs[t*iterations + i] = workload.operation_count*1000*(t+1)/client.run_map_locked(shared_map, t+1, ops);
                std::cout << "Iteration: " << i << ": " <<  throughputs[t*iterations + i] << std::endl;
            }
            // Calculating the performance
            thread_total_throughput = 0;
            for(std::uint32_t i = 0; i < iterations; i++) 
    	        thread_total_throughput += throughputs[t*iterations + i];
            double mean = thread_total_throughput / iterations;
            double var = 0.0;

            for(std::uint32_t i = 0; i < iterations; i++)
            {
                var += (throughputs[t*iterations + i]-mean)*(throughputs[t*iterations + i]-mean);
            }

            var /= iterations;
            double std_dev = std::sqrt(var);
            std::cout << "Avg Throughput: " << mean << " operations/second." << std::endl;
            std::cout << "Std Deviation:  " << std_dev << " operations/second." << std::endl;

            // Writing data to file
            out_file << (t+1) << "\t" << mean << "\t" << std_dev << "\n";
            std::cout << "Data written" << std::endl;
	}
	return;
    } 

    client.run_build_records(1);
    std::cout << "Records built" << std::endl;
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
    for(std::uint32_t t = 0; t < thread_count; t++) {
        std::cout << "Thread count: " << t << ": " << std::endl;
        for(std::uint32_t i = 0; i < iterations; i++) 
        {
            throughputs[t*iterations + i] = workload.operation_count*1000*(t+1)/client.run_transactions(t+1);
            std::cout << "Iteration: " << i << ": " <<  throughputs[t*iterations + i] << std::endl;
        }
        // Calculating the performance
        thread_total_throughput = 0;
        for(std::uint32_t i = 0; i < iterations; i++) 
            thread_total_throughput += throughputs[t*iterations + i];
        double mean = thread_total_throughput / iterations;
        double var = 0.0;
        for(std::uint32_t i = 0; i < iterations; i++)
        {
            var += (throughputs[t*iterations + i]-mean)*(throughputs[t*iterations + i]-mean);
        }
        var /= iterations;
        double std_dev = std::sqrt(var);
        std::cout << "Avg Throughput: " << mean << " operations/second." << std::endl;
        std::cout << "Std Deviation:  " << std_dev << " operations/second." << std::endl;

        // Writing data to file
        out_file << (t+1) << "\t" << mean << "\t" << std_dev << "\n";
        std::cout << "Data written" << std::endl;
	std::this_thread::sleep_for(std::chrono::seconds(2));
    }
    out_file.flush();
    if (out_file.fail())
      std::cout << "Something failed" << std::endl;
    out_file.close();

    if (hash_table) delete hash_table;
    if (hash) delete hash;
}

int main(int argc, char *argv[]) {
    std::string workload_string = "workload_a";
    std::uint8_t hash_func_num  = 2;
    std::uint8_t hash_index_num = 0;
    std::uint8_t thread_count   = 16;
    std::string file_suffix = "";

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
    workload_string = argv[1];
    hash_func_num   = (std::uint8_t)(argv[2][0]-'0');
    hash_index_num  = (std::uint8_t)(argv[3][0]-'0');
    thread_count    = std::atoi(argv[4]);

    if (argc > 5)
	file_suffix = argv[5];

    test_workload(thread_count, workload_string, hash_func_num, hash_index_num, file_suffix);
}
