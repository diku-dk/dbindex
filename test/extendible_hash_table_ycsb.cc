#include <string>
#include <chrono>
#include <iostream>

#include "../src/hash_functions/mod_hash.h"
#include "../src/hash_index/array_hash_table.h"
#include "../src/benchmarks/ycsb/client.h"
#include "../src/benchmarks/ycsb/core_workloads.h"

typedef std::uint32_t hash_value_t;

void test_workload_a(std::uint8_t thread_count) {
    using namespace std::chrono;

    constexpr size_t directory_size = 1<<10;
    constexpr std::uint32_t mod_value = 1<<31;

    const ycsb::workload_properties workload_a = {
        100000, 100000, 
        0.5, 0.5, 0, 0, 0, 
        ycsb::distribution_type::ZIPFIAN, ycsb::distribution_type::UNUSED, 
        0, 100
    };

    dbindex::mod_hash<hash_value_t, mod_value> hash = dbindex::mod_hash<hash_value_t, mod_value>();

    dbindex::array_hash_table<directory_size> hash_table = dbindex::array_hash_table<directory_size>(&hash);

    ycsb::client client(hash_table, workload_a, thread_count);


    client.run_build_records(thread_count);
    // Calculating the hashing
    std::uint32_t duration;
    std::uint32_t iterations = 20;

    for(std::uint32_t t = 1; t < 5; t++) {
        std::cout << "Thread count: " << t << std::endl;
        duration = 0;
        for(std::uint32_t j = 0; j < iterations; j++) 
        {
            auto start = high_resolution_clock::now();
            client.run_transactions(t);

            auto end = high_resolution_clock::now();
            duration += duration_cast<milliseconds>(end-start).count();
            std::cout << duration_cast<milliseconds>(end-start).count() << std::endl;
        }
        std::cout << "Avg: " << (duration / iterations) << std::endl;
    }
}

int main(int argc, char *argv[]) {
    test_workload_a(1);
}