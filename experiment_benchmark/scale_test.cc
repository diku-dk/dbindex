#include <iostream>
#include <thread>
#include <pthread.h>
#include <chrono>

int stick_thread_to_core(pthread_t thread, int core_id) {
   	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
	CPU_SET(core_id, &cpuset);
	
	return pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
}

void do_data_gen_timed(std::uint32_t t) {
	std::cout << (int)stick_thread_to_core(pthread_self(), (t*2)%32 + ((t*2)/32)) << std::endl;
	std::uint64_t operation_count = 10000000000;
	double result = 0;
        auto start = std::chrono::high_resolution_clock::now();
        for (std::uint64_t i = 0; i < operation_count; i++) {
               	result += (double)i;
        }
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end-start).count();
	auto throughput = operation_count*1000/duration;
        std::cout << throughput << ", " << duration << ", " << result << std::endl;
}

int main(int argc, char *argv[]) {
	std::thread threads[32];

	for (std::uint32_t i = 0; i < 32; i++) {
		std::cout << "Using " << i << " threads!" << std::endl;
		// Insertions 	- Initialization of the index
		for(std::uint32_t t = 0; t <= i; t++) {
			threads[t] = std::thread(do_data_gen_timed, t);
		}
		for(std::uint32_t t = 0; t <= i; t++) {
			threads[t].join();
		}
	}
}
