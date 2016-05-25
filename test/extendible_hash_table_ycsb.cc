#include <string>
#include <chrono>
#include <iostream>

#include "../src/hash_functions/mod_hash.h"
#include "../src/hash_functions/mult_shift_hash.h"
#include "../src/hash_functions/tabulation_hash.h"
#include "../src/hash_functions/murmur_hash_32.h"
#include "../src/hash_index/array_hash_table.h"
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

void test_workload_a(std::uint8_t thread_count, std::string workload_string) {
    using namespace std::chrono;

    constexpr size_t directory_size = 1<<10;
    constexpr std::uint32_t mod_value = 1<<31;
    constexpr std::uint32_t MAX_KEY_LEN_VAL = 64;

    ycsb::workload_properties workload = parse_workload_string(workload_string);
    // const ycsb::workload_properties workload_a = {
    //     100000, 100000, 
    //     0.5, 0.5, 0, 0, 0, 
    //     ycsb::distribution_type::ZIPFIAN, ycsb::distribution_type::UNUSED, 
    //     0, 100
    // };

    dbindex::mod_hash<hash_value_t, mod_value> mod_hash              = dbindex::mod_hash<hash_value_t, mod_value>();
    dbindex::mult_shift_hash<hash_value_t> mult_hash                 = dbindex::mult_shift_hash<hash_value_t>();
    dbindex::murmur_hash_32<hash_value_t> murmur_hash                = dbindex::murmur_hash_32<hash_value_t>();
    dbindex::tabulation_hash<hash_value_t, MAX_KEY_LEN_VAL> tab_hash = dbindex::tabulation_hash<hash_value_t, MAX_KEY_LEN_VAL>();

    // dbindex::array_hash_table<directory_size> hash_table = dbindex::array_hash_table<directory_size>(&mod_hash); std::cout << "Using mod_hash" << std::endl;
    // dbindex::array_hash_table<directory_size> hash_table = dbindex::array_hash_table<directory_size>(&mult_hash); std::cout << "Using mult_hash" << std::endl;
    // dbindex::array_hash_table<directory_size> hash_table = dbindex::array_hash_table<directory_size>(&murmur_hash); std::cout << "Using murmur_hash" << std::endl;
    dbindex::array_hash_table<directory_size> hash_table = dbindex::array_hash_table<directory_size>(&tab_hash); std::cout << "Using tab_hash" << std::endl;

    ycsb::client client(hash_table, workload, thread_count);


    client.run_build_records(thread_count);
    // Calculating the hashing
    std::uint32_t total_duration;
    std::uint32_t duration;
    std::uint32_t iterations = 50;

    std::cout << "Running benchmark with " << workload_to_string(workload) << std::endl;
    for(std::uint32_t t = 1; t <= 16; t++) {
        std::cout << "Thread count: " << t << ": ";
        total_duration = 0;
        for(std::uint32_t j = 0; j < iterations; j++) 
        {
            duration = client.run_transactions(t);
            total_duration += duration;
            // std::cout << duration << std::endl;
        }
        std::cout << "Avg: " << (total_duration / iterations) << std::endl;
    }
}

int main(int argc, char *argv[]) {
    std::string workload_string = "workload_a";

    if (argc > 1)
        workload_string = argv[1];


    test_workload_a(1, workload_string);
}