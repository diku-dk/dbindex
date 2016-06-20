#include "../../util/thread_util.h"
#include "../../abstract_index.h"
#include "workload.h"
#include "client.h"
#include <thread> 
#include <iostream>
#include <chrono> 

namespace ycsb {

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

void do_insertions_concurrent_timed(dbindex::abstract_index& hash_index, workload& wl, std::uint32_t record_count, utils::timing_obj& timing) {
	timing.set_start(std::chrono::high_resolution_clock::now());
	for (std::uint32_t i = 0; i < record_count; i++) {
		wl.do_insert(hash_index);
	}
	timing.set_end(std::chrono::high_resolution_clock::now());
}
void do_transactions_concurrent_timed(dbindex::abstract_index& hash_index, workload& wl, std::uint32_t operation_count, utils::timing_obj& timing) {
	timing.set_start(std::chrono::high_resolution_clock::now());
	for (std::uint32_t i = 0; i < operation_count; i++) {
		wl.do_transaction(hash_index);
	}
	timing.set_end(std::chrono::high_resolution_clock::now());
}

void client::run_build_records(std::uint8_t thread_count) {
	std::thread threads[thread_count];
	std::uint32_t thread_record_count = wl.get_record_count() / thread_count;

	// Insertions 	- Initialization of the index
	for(std::uint32_t t = 0; t < thread_count; t++) {
		threads[t] = std::thread(do_insertions_concurrent, std::ref(hash_index), std::ref(wl), thread_record_count);
		utils::stick_thread_to_core(threads[t].native_handle(), (t*2)+1);
	}
	for(std::uint32_t t = 0; t < thread_count; t++) {
		threads[t].join();
	}
}

std::uint32_t client::run_transactions(std::uint8_t thread_count) {
	std::thread threads[thread_count];
	utils::timing_obj timings[thread_count];
	
	std::uint32_t thread_operation_count = wl.get_operation_count() / thread_count;
	// Transactions - Running the designed workload.
	for(std::uint32_t t = 0; t < thread_count; t++) {
		threads[t] = std::thread(do_transactions_concurrent_timed, std::ref(hash_index), std::ref(wl), thread_operation_count, std::ref(timings[t]));
		utils::stick_thread_to_core(threads[t].native_handle(), (t*2)+1);
	}
	for(std::uint32_t t = 0; t < thread_count; t++) {
		threads[t].join();
	}

	std::chrono::_V2::system_clock::time_point min_start = timings[0].start;
	std::chrono::_V2::system_clock::time_point max_end = timings[0].end;
        // std::cout << std::chrono::duration_cast<std::chrono::milliseconds>(timings[0].end - timings[0].start).count() << std::endl;

	for(std::uint32_t t = 1; t < thread_count; t++) {
		if (timings[t].start < min_start) {
			min_start = timings[t].start;
		}
		if (timings[t].end > max_end) {
			max_end = timings[t].end;
		}
	        // std::cout << std::chrono::duration_cast<std::chrono::milliseconds>(timings[t].end - timings[t].start).count() << std::endl;
	}
	return std::chrono::duration_cast<std::chrono::milliseconds>(max_end-min_start).count();
}


std::uint32_t client::run_workload() {
	std::thread threads[thread_count];
	utils::timing_obj timings[thread_count];

	std::uint32_t thread_record_count    = wl.get_record_count() / thread_count;
	std::uint32_t thread_operation_count = wl.get_operation_count() / thread_count;

	// Insertions 	- Initialization of the index
	for(std::uint32_t t = 0; t < thread_count; t++) {
		threads[t] = std::thread(do_insertions_concurrent_timed, std::ref(hash_index), std::ref(wl), thread_record_count, std::ref(timings[t]));
		utils::stick_thread_to_core(threads[t].native_handle(), (t*2)+1);
	}
	for(std::uint32_t t = 0; t < thread_count; t++) {
		threads[t].join();
	}

	// Transactions - Running the designed workload.
	for(std::uint32_t t = 0; t < thread_count; t++) {
		threads[t] = std::thread(do_transactions_concurrent_timed, std::ref(hash_index), std::ref(wl), thread_operation_count, std::ref(timings[t]));
		utils::stick_thread_to_core(threads[t].native_handle(), (t*2)+1);
	}
	for(std::uint32_t t = 0; t < thread_count; t++) {
		threads[t].join();
	}

	std::chrono::_V2::system_clock::time_point min_start = timings[0].start;
	std::chrono::_V2::system_clock::time_point max_end = timings[0].end;

	for(std::uint32_t t = 1; t < thread_count; t++) {
		if (timings[t].start < min_start) {
			min_start = timings[t].start;
		}
		if (timings[t].end > max_end) {
			max_end = timings[t].end;
		}
	}
	return std::chrono::duration_cast<std::chrono::milliseconds>(max_end-min_start).count();
}

}
