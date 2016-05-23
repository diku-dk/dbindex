#include "../../util/thread_util.h"
#include "../../abstract_index.h"
#include "workload.h"
#include "client.h"
#include <thread> 

namespace ycsb {

void do_insertions_concurrent(dbindex::abstract_index *hash_index, workload& wl, std::uint32_t record_count) {
	for (std::uint32_t i = 0; i < record_count; i++) {
		wl.do_insert(hash_index);
	}
}


void do_transactions_concurrent(dbindex::abstract_index *hash_index, workload& wl, std::uint32_t operation_count) {
	for (std::uint32_t i = 0; i < operation_count; i++) {
		wl.do_transaction(hash_index);
	}
}
void client::run_build_records(std::uint8_t thread_count) {

	std::thread threads[thread_count];

	std::uint32_t thread_record_count = wl.get_record_count() / thread_count;


	// Insertions 	- Initialization of the index
	for(std::uint32_t t = 0; t < thread_count; t++) {
		threads[t] = std::thread(do_insertions_concurrent, &hash_index, std::ref(wl), thread_record_count);
		utils::stick_thread_to_core(threads[t].native_handle(), (t*2)+1);
	}
	for(std::uint32_t t = 0; t < thread_count; t++) {
		threads[t].join();
	}
}
void client::run_transactions(std::uint8_t thread_count) {
	std::thread threads[thread_count];
	
	std::uint32_t thread_operation_count = wl.get_operation_count() / thread_count;
	// Transactions - Running the designed workload.
	for(std::uint32_t t = 0; t < thread_count; t++) {
		threads[t] = std::thread(do_transactions_concurrent, &hash_index, std::ref(wl), thread_operation_count);
		// utils::stick_thread_to_core(threads[t].native_handle(), (t*2)+1);
	}
	for(std::uint32_t t = 0; t < thread_count; t++) {
		threads[t].join();
	}
}


void client::run_workload() {
	
	std::thread threads[thread_count];

	std::uint32_t record_count    = wl.get_record_count();
	std::uint32_t operation_count = wl.get_operation_count();

	// Insertions 	- Initialization of the index
	for(std::uint32_t t = 0; t < thread_count; t++) {
		threads[t] = std::thread(do_insertions_concurrent, &hash_index, std::ref(wl), record_count);
		// utils::stick_thread_to_core(threads[t].native_handle(), (t*2)+1);
	}
	for(std::uint32_t t = 0; t < thread_count; t++) {
		threads[t].join();
	}

	// Transactions - Running the designed workload.
	for(std::uint32_t t = 0; t < thread_count; t++) {
		threads[t] = std::thread(do_transactions_concurrent, &hash_index, std::ref(wl), operation_count);
		// utils::stick_thread_to_core(threads[t].native_handle(), (t*2)+1);
	}
	for(std::uint32_t t = 0; t < thread_count; t++) {
		threads[t].join();
	}
}

}