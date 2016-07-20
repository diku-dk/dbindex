#include "../src/hash_functions/tabulation_hash.h"
#include "../src/hash_functions/murmur_hash_32.h"
#include "../src/hash_functions/abstract_hash.h"
#include "../src/util/thread_util.h"

#include <string>
#include <chrono>
#include <iostream>
#include <fstream>
#include <iomanip>
#include <algorithm>
#include <climits>
#include <cstring>
#include <map>
#include <assert.h>
#include <thread>
#include <pthread.h>
           

int stick_thread_to_core(pthread_t thread, int core_id) {
   cpu_set_t cpuset;
   CPU_ZERO(&cpuset);
   CPU_SET(core_id, &cpuset);

   return pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
}

void generate_random_strings_length(std::uint8_t byte_len, std::vector<std::string>& keys) {
    std::random_device rd;
    std::mt19937_64 generator(rd());
    std::uniform_int_distribution<std::uint8_t> distribution(1, 255);

    for (std::uint32_t i = 0; i < keys.size(); i++) {
        keys[i] = "";
        for (std::uint32_t c = 0; c < byte_len; c++) {
            keys[i] += distribution(generator);
        }
    }
}

/**
 * Tests the performance of the hashing, over different string lengths, using normally distributed keys.
 * Returns the total amount of nanoseconds spent.
 */
void test_length_performance(dbindex::abstract_hash<std::uint32_t>* hash, std::uint32_t amount, const std::uint8_t max_key_len, std::ofstream& out_file) {
    using namespace std::chrono;

    const std::uint32_t iterations          = 10000;
    const std::uint32_t repeats             = 10000;
    const std::uint8_t  stride              = 1;
    const std::uint8_t  used_length_amounts = max_key_len/stride;

    std::vector<std::vector<std::uint32_t>> times(iterations, std::vector<std::uint32_t>(used_length_amounts));


    auto start = high_resolution_clock::now();
    auto end = high_resolution_clock::now();

    // Testing
    std::vector<std::string> keys(amount);
    for (std::uint32_t i = 0; i < iterations; i++) {
        std::cout << "Iteration " << (int)(i+1) << " of " << (int)iterations << std::endl;
        // Pre-generating the strings, to only read time of actual hashing

        for (std::uint8_t k = 0; k < used_length_amounts; k++) {  
            generate_random_strings_length((k+1)*stride, keys);

            // Warmup 
            start = high_resolution_clock::now();
            for(std::uint32_t j = 0; j < amount; j++)
                hash->get_hash(keys[j]);

            start = high_resolution_clock::now();
            // Calculating the hashing
            for (std::uint32_t c = 0; c < repeats; c++) {    
                for(std::uint32_t j = 0; j < amount; j++) {
                    hash->get_hash(keys[j]);
                }
            }
            end = high_resolution_clock::now();
            times[i][k] = duration_cast<nanoseconds>((end-start)/(amount*repeats)).count();
        }
    }  
    std::vector<double> time_means(used_length_amounts, 0);
    std::vector<double> time_vars(used_length_amounts, 0);

    // Sum
    for (std::uint32_t i = 0; i < iterations; i++)
        for (std::uint8_t k = 0; k < used_length_amounts; k++)
            time_means[k] += times[i][k];

    // Mean
    for (std::uint8_t k = 0; k < used_length_amounts; k++)
        time_means[k] = time_means[k]/iterations;

    // Variance
    for (std::uint32_t i = 0; i < iterations; i++)
        for (std::uint8_t k = 0; k < used_length_amounts; k++)
            time_vars[k] += (times[i][k] - time_means[k]) * (times[i][k] - time_means[k]);

    for (std::uint8_t k = 0; k < used_length_amounts; k++)
        out_file << std::to_string((k*stride)+1) << "\t" << std::to_string(time_means[k]) << "\t" << std::to_string(sqrt((double)time_vars[k]/iterations)) << "\n";
    std::cout << "Written to file" << std::endl;
}

void test_core_performance(dbindex::abstract_hash<std::uint32_t>* hash, std::string *keys, std::uint32_t amount, std::uint32_t iterations, utils::timing_obj& timing, std::uint8_t t) {
    using namespace std::chrono;
    stick_thread_to_core(pthread_self(), (t*2)+1);

    // Warmup 
    timing.set_start(high_resolution_clock::now());
    for(std::uint32_t j = 0; j < amount; j++)
        hash->get_hash(keys[j]);
    timing.set_end(high_resolution_clock::now());

    timing.set_start(high_resolution_clock::now());
    for (std::uint32_t i = 0; i < iterations; i++)
        for(std::uint32_t j = 0; j < amount; j++) 
            hash->get_hash(keys[j]);
    timing.set_end(high_resolution_clock::now());
}

std::uint64_t test_cores_performance(dbindex::abstract_hash<std::uint32_t>* hash, std::uint8_t num_threads, std::uint8_t byte_len, std::uint32_t amount) {

    // Testing
    std::vector<std::string> keys(amount*num_threads);
    generate_random_strings_length(byte_len, keys);

    std::vector<std::thread>       threads(num_threads);
    std::vector<utils::timing_obj> timings(num_threads);
    std::uint32_t iterations = 10000000;

    // Calculating the hashing
    for(std::uint32_t t = 0; t < num_threads; t++) {
        threads[t] = std::thread(test_core_performance, hash, &keys[t*amount], amount, iterations, std::ref(timings[t]), t);
    }
    for(std::uint32_t t = 0; t < num_threads; t++) {
        threads[t].join();
    }

    std::uint64_t total_time = 0;
    for (std::uint8_t t = 0; t < num_threads; t++) {
       total_time += timings[t].get_duration_milliseconds();
    }

    std::cout << "Time per thread: " << total_time/num_threads << "ms / " << (((uint64_t)amount*iterations*num_threads)*1000)/total_time << " TP." <<  std::endl;
    std::cout << "Total CPU Time:  " << total_time << "ms / " << (((uint64_t)amount*iterations)*num_threads*num_threads*1000)/total_time << " TP."<<  std::endl;

    return (((uint64_t)amount*iterations*num_threads*num_threads*1000))/total_time;
}

int main(int argc, char *argv[]) {
    using namespace std::chrono;

    // Argv[1] = Hash type
    // Argv[2] = Test type

    if (argc < 3)
    {
        std::cout << "You must provide Hash function to use and Test to run" << std::endl;
        std::cout << "Hash function options:" << std::endl;
        std::cout << "\t- 1: Murmur" << std::endl;
        std::cout << "\t- 2: Tabulation" << std::endl;
        std::cout << std::endl;
        std::cout << "Test optionskey_len_bytes:" << std::endl;
        std::cout << "\t- 3: Length" << std::endl;
        std::cout << "\t- 4: Cores" << std::endl;
        return 0;
    }                              
  
    std::string test_type;
    std::string hash_type;

    dbindex::abstract_hash<std::uint32_t> *hash;

    /************************************************/
    /*********** Mult_Shift_Hash testing ************/
    /************************************************/

    const std::uint8_t num_tables = 16;

    switch ((int)(argv[1][0]-'0')) {
    case 1:   
        hash_type = "Murmur"; 
        hash = new dbindex::murmur_hash_32<std::uint32_t>();
        break;
    case 2:   
        hash_type = "Tabulation"; 
        hash = new dbindex::tabulation_hash<std::uint32_t, num_tables>();
        break;
    default: 
        std::cout << "Unknown Hash Type" << std::endl;
        return 0;
    }

    std::cout << hash_type << " Hashing Test" << std::endl;
    /****************************************/
    /*********** General testing ************/
    /****************************************/
    std::ofstream out_file;
    // std::ifstream in_file;
    // char * in_buffer;
    // std::string in_str;
    // std::uint32_t in_size;
    std::uint64_t thread_tp;

    std::map<std::uint32_t, std::uint32_t> hist;

    std::uint32_t len_amount   = 5;
    std::uint8_t  core_key_len = 64;
    std::uint8_t  num_threads  = 8;

    switch ((int)(argv[2][0]-'0')) {
    case 3: 
        test_type = "_length";
        out_file.open ("../Thesis/src/Results/Length/" + hash_type + "_" + std::to_string(num_tables) + "_tables" + test_type + ".txt");
        test_length_performance(hash, len_amount, num_tables, out_file);
        std::cout << "../Thesis/src/Results/Length/" + hash_type + "_" + std::to_string(num_tables) + "_tables" + test_type + ".txt" << std::endl;
        out_file.flush();
        out_file.close();
        std::cout << "outfile closed" << std::endl;
        break;
    case 4: 
        test_type = "_cores";
        // if (num_threads != 1)
        // {
        //     in_file.open ("../Thesis/src/Results/Cores/" + hash_type + "_" + std::to_string(num_tables) + "_tables" + test_type + ".txt");

        //     // get size of file
        //     in_file.seekg(0,std::ifstream::end);
        //     in_size=in_file.tellg();
        //     in_file.seekg(0);

        //     // allocate memory for file content
        //     in_buffer = new char [in_size];

        //     // read content of in_file
        //     in_file.read (in_buffer,in_size);
        //     in_str = std::string(in_buffer, in_size);
        //     in_file.close();
        // }
        out_file.open ("../Thesis/src/Results/Cores/" + hash_type + "_" + std::to_string(num_tables) + "_tables" + test_type + ".txt");
        std::cout << "../Thesis/src/Results/Cores/" + hash_type + "_" + std::to_string(num_tables) + "_tables" + test_type + ".txt" << std::endl;
        for (std::uint8_t tc = 1; tc <= num_threads; tc++) {
            thread_tp = test_cores_performance(hash, tc, core_key_len, len_amount);
            out_file << (int)tc << "\t" << thread_tp << std::endl;
        }
        out_file.flush();
        out_file.close();
        break;
    default:  
        std::cout << "Unknown Test Type" << std::endl;
        return 0;
    }

    // end = high_resolution_clock::now();
    // std::cout << "Total test time: " << duration_cast<microseconds>((end-start)/1000).count() << "ms" << std::endl;
    delete hash;
    return 0;
}
