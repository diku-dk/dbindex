#include "../../util/thread_util.h"
#include "../../macros.h"
#include "../../abstract_index.h"
#include "workload.h"
#include "client.h"
#include <thread> 
#include <iostream>
#include <chrono> 

namespace ycsb {

std::uint8_t calc_cpu(std::uint8_t t) {
	return (t*8 + (t/4)*2 + t/16) % 32;
}

void do_insertions_concurrent(dbindex::abstract_index& hash_index, workload& wl, std::uint32_t record_count) {
	for (std::uint32_t i = 0; i < record_count; i++) {
		// std::cout << ((double)i*100)/((double)record_count) << "%" << std::endl;
		wl.do_insert(hash_index);
	}
}
void do_transactions_concurrent(dbindex::abstract_index& hash_index, workload& wl, std::uint32_t operation_count) {
	for (std::uint32_t i = 0; i < operation_count; i++) {
		wl.do_transaction(hash_index);
	}
}

void do_insertions_concurrent_timed(dbindex::abstract_index& hash_index, workload& wl, std::uint32_t record_count, utils::timing_obj& timing, std::uint8_t t) {
    stick_thread_to_core(pthread_self(), calc_cpu(t));

	timing.set_start(std::chrono::high_resolution_clock::now());
	for (std::uint32_t i = 0; i < record_count; i++) {
		wl.do_insert(hash_index);
	}
	timing.set_end(std::chrono::high_resolution_clock::now());
}
void do_transactions_concurrent_timed(dbindex::abstract_index& hash_index, workload& wl, std::uint32_t operation_count, utils::timing_obj& timing, std::uint8_t t) {
    stick_thread_to_core(pthread_self(), calc_cpu(t));

	timing.set_start(std::chrono::high_resolution_clock::now());
	for (std::uint32_t i = 0; i < operation_count; i++) {
		// std::cout << ((double)i*100)/((double)operation_count) << "%" << std::endl;
		wl.do_transaction(hash_index);
	}
	timing.set_end(std::chrono::high_resolution_clock::now());
	//std::cout << "Thread Throughput: " << operation_count*1000/timing.get_duration_milliseconds() << std::endl;

}

void client::run_build_records(std::uint8_t thread_count) {
	std::thread threads[thread_count];
	workload wls[thread_count];

	// Insertions 	- Initialization of the index
	for(std::uint32_t t = 0; t < thread_count; t++) {
		wls[t].init(wl_p);
		threads[t] = std::thread(do_insertions_concurrent, std::ref(hash_index), std::ref(wls[t]), wls[t].get_record_count());
		utils::stick_thread_to_core(threads[t].native_handle(), calc_cpu(t));
	}
	for(std::uint32_t t = 0; t < thread_count; t++) {
		threads[t].join();
	}
}

std::uint32_t client::run_transactions(std::uint8_t thread_count) {
	std::thread threads[thread_count];
	utils::timing_obj timings[thread_count];
	workload wls[thread_count];

	// Insertions 	- Initialization of the index
	for(std::uint32_t t = 0; t < thread_count; t++) {
		wls[t].init(wl_p);
		threads[t] = std::thread(do_transactions_concurrent_timed, std::ref(hash_index), std::ref(wls[t]), wls[t].get_operation_count(), std::ref(timings[t]), t);
	}
	for(std::uint32_t t = 0; t < thread_count; t++) {
		threads[t].join();
	}

	return calc_throughput(timings, wls, thread_count);
}

std::uint32_t client::run_workload(std::uint8_t thread_count) {
	std::thread threads[thread_count];
	utils::timing_obj timings[thread_count];
	workload wls[thread_count];
	// Insertions 	- Initialization of the index
	for(std::uint32_t t = 0; t < thread_count; t++) {
		wls[t].init(wl_p);
		threads[t] = std::thread(do_insertions_concurrent_timed, std::ref(hash_index), std::ref(wls[t]), wls[t].get_record_count(), std::ref(timings[t]), t);
	}
	for(std::uint32_t t = 0; t < thread_count; t++) {
		threads[t].join();
	}

	// Transactions - Running the designed workload.
	for(std::uint32_t t = 0; t < thread_count; t++) {
		threads[t] = std::thread(do_transactions_concurrent_timed, std::ref(hash_index), std::ref(wls[t]), wls[t].get_operation_count(), std::ref(timings[t]), t);
	}
	for(std::uint32_t t = 0; t < thread_count; t++) {
		threads[t].join();
	}

	return calc_throughput(timings, wls, thread_count);
}


std::uint32_t client::calc_throughput(utils::timing_obj timings[], workload wls[], std::uint8_t thread_count) {
	// std::chrono::_V2::system_clock::time_point min_start = timings[0].start;
	// std::chrono::_V2::system_clock::time_point max_end = timings[0].end;
	// std::cout << std::chrono::duration_cast<std::chrono::milliseconds>(timings[0].end - timings[0].start).count() << std::endl;

	std::uint32_t total_throughput = 0;
	for(std::uint32_t t = 0; t < thread_count; t++) {
		std::cout << wls[t].get_operation_count()*1000/timings[t].get_duration_milliseconds() << std::endl;
		total_throughput += wls[t].get_operation_count()*1000/timings[t].get_duration_milliseconds();
		// if (timings[t].start < min_start) {
		// 	min_start = timings[t].start;
		// }
		// if (timings[t].end > max_end) {
		// 	max_end = timings[t].end;
		// }
		// std::cout << std::chrono::duration_cast<std::chrono::milliseconds>(timings[t].end - timings[t].start).count() << std::endl;
	}
	return total_throughput; 
	// return std::chrono::duration_cast<std::chrono::milliseconds>(max_end-min_start).count();
}

}
