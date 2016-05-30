#include <string>
#include <chrono>
#include <fstream>
#include <iostream>

#include "../src/hash_functions/abstract_hash.h"
#include "../src/hash_functions/mod_hash.h"
#include "../src/hash_functions/mult_shift_hash.h"
#include "../src/hash_functions/tabulation_hash.h"
#include "../src/hash_functions/murmur_hash_32.h"
#include "../src/hash_index/array_hash_table.h"
#include "../src/hash_index/extendible_hash_table.h"
#include "../src/benchmarks/ycsb/client.h"
#include "../src/benchmarks/ycsb/core_workloads.h"

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

    std::cout << "Unknown workload, returning workload_a" << std::endl;
    return "workload_a";
}

void test_workload_a(std::uint8_t thread_count, std::string workload_string, std::uint8_t hash_func_num, std::uint8_t hash_index_num) {
    using namespace std::chrono;

    constexpr size_t directory_size = 1<<10;
    constexpr size_t initial_global_depht = 2;
    constexpr std::uint32_t mod_value = 1<<31;
    constexpr std::uint32_t MAX_KEY_LEN_VAL = 14;

    ycsb::workload_properties workload = parse_workload_string(workload_string);


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
        hash_func_string = "murmur_hash_32";
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
    default:
        std::cout << "Unknown hash_index_num: \"" << hash_index_num << "\"." << std::endl;
        hash_index_string = "extendible_hash_table";
        hash_table = new dbindex::extendible_hash_table<initial_global_depht>(*hash);
        break;
    }
    std::cout << "Using " << hash_func_string << " and " << hash_index_string << std::endl;

    ycsb::client client(*hash_table, workload, thread_count);
    client.run_build_records(thread_count);

    // Calculating the hashing
    std::uint32_t iterations = 5;

    std::uint32_t thread_total_duration;
    std::uint32_t durations[iterations*thread_count];

        // Opening Data File
    std::ofstream out_file;
    std::string path = "results/" + hash_func_string + "_" + hash_index_string + "_" + workload_string + ".txt";
    std::cout << path << std::endl;
    out_file.open (path);
    out_file.clear();
    if (!out_file.is_open()) {
        std::cout << "Data file isn't open." << std::endl;
        return;
    }

        // Running experiments 
    std::cout << "Running benchmark with " << workload_to_string(workload) << std::endl;
    for(std::uint32_t t = 1; t <= thread_count; t++) {
        std::cout << "Thread count: " << t << ": " << std::endl;
        for(std::uint32_t i = 0; i < iterations; i++) 
        {
            durations[(t-1)*iterations + i] = client.run_transactions(t);;
            // std::cout << i << ": " <<  durations[(t-1)*iterations + i] << std::endl;
        }
        // Calculating the performance
        thread_total_duration = 0;
        for(std::uint32_t i = 0; i < iterations; i++) 
            thread_total_duration += durations[(t-1)*iterations + i];
        std::cout << "Avg: " << (thread_total_duration / iterations) << std::endl;

        // Writing data to file
        out_file << t << "\t" << (thread_total_duration / iterations) << "\n";
    }

    out_file.flush();
    if (out_file.fail())
      std::cout << "Something failed" << std::endl;
    out_file.close();

    delete hash;
    delete hash_table;
}

int main(int argc, char *argv[]) {
    std::string workload_string = "workload_a";
    std::uint8_t hash_func_num  = 2;
    std::uint8_t hash_index_num = 0;
    std::uint8_t thread_count   = 4;

    if (argc > 1)
        workload_string = argv[1];

    if (argc > 2)
        hash_func_num = (std::uint8_t)(argv[2][0]-'0');

    if (argc > 3)
        hash_index_num = (std::uint8_t)(argv[3][0]-'0');


    test_workload_a(thread_count, workload_string, hash_func_num, hash_index_num);
}