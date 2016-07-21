#include <iostream>
#include <thread>
#include <pthread.h>
#include <chrono>
#include <vector>
#include <iostream>
#include <fstream>
#include <algorithm>

int stick_thread_to_core(pthread_t thread, int core_id) {
   	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
	CPU_SET(core_id, &cpuset);
	
	return pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
}

int calc_cpu(std::uint32_t t) {
	return (t*4 + (t/8) * 2 + t/16) % 32;
}

void do_data_gen_timed(std::uint32_t t, std::uint32_t& throughput_res) {
	stick_thread_to_core(pthread_self(), calc_cpu(t));
	// std::cout << "Using cpu: " << calc_cpu(t) << ", " << affinity_result << std::endl;
	std::uint64_t operation_count = 10000000000;
	double result = 0;
    auto start = std::chrono::high_resolution_clock::now();
    for (std::uint64_t i = 0; i < operation_count; i++) {
           	result += (double)i;
    }
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end-start).count();
	throughput_res = operation_count*1000/duration;
   	//std::cout << "Result of " << calc_cpu(t) << ":\t" << throughput << ", " << duration << ", " << result << ", " << (affinity_result ? "Failed" : "Success") << std::endl;
}

int main(int argc, char *argv[]) {
	std::uint8_t num_threads = 32;
	std::thread threads[num_threads];


    std::ofstream out_file;
    out_file.open ("../Thesis/src/baseline_scaling.txt");
	std::vector<std::uint32_t> throughput_ress(num_threads);

	for (std::uint32_t i = 1; i <= num_threads; i++) {
		std::cout << "\nUsing " << i << " threads!" << std::endl;
		// Insertions 	- Initialization of the index
		for(std::uint32_t t = 0; t < i; t++) {
			threads[t] = std::thread(do_data_gen_timed, t, std::ref(throughput_ress[t]));
		}
		for(std::uint32_t t = 0; t < i; t++) {
			threads[t].join();
		}

		double tp_mean = 0;
		double tp_var = 0;

		// Mean
	    for (std::uint32_t t = 0; t < i; t++)
	        tp_mean += throughput_ress[t];

	    tp_mean /= i;

	    // Variance
	    for (std::uint32_t t = 0; t < i; t++)
	        tp_var += (throughput_ress[t] - tp_mean) * (throughput_ress[t] - tp_mean);

	    tp_var /= i;

	    out_file << std::to_string(i) << "\t" << std::to_string(tp_mean) << "\t" << std::to_string(sqrt(tp_var)) << "\n";
    	out_file.flush();		
	}
    out_file.close();
}
