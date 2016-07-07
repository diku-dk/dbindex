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
		// std::cout << ((double)i*100)/((double)operation_count) << "%" << std::endl;
		wl.do_transaction(hash_index);
	}
	timing.set_end(std::chrono::high_resolution_clock::now());
	//std::cout << "Thread Throughput: " << operation_count*1000/timing.get_duration_milliseconds() << std::endl;

}
/********* Debugging only, Do delete ***********/

void do_locks_timed(boost::shared_mutex& shared_mutex, std::uint32_t operation_count, utils::timing_obj& timing) {
	boost::shared_lock<boost::shared_mutex> local_shared_lock(shared_mutex, boost::defer_lock);
	timing.set_start(std::chrono::high_resolution_clock::now());
	for (std::uint32_t i = 0; i < operation_count; i++) {
		local_shared_lock.lock();
		local_shared_lock.unlock();
	}
	timing.set_end(std::chrono::high_resolution_clock::now());
}

void do_map_insertions_concurrent(std::map<std::string, std::string>& map, workload& wl, std::uint32_t record_count) {
	for (std::uint32_t i = 0; i < record_count; i++) {
		// std::cout << ((double)i*100)/((double)record_count) << "%" << std::endl;
		std::string key = wl.next_sequence_key();
	    std::string value;
	    wl.build_value(value);
		map.insert(std::pair<std::string, std::string>(key, value));
	}
}
void do_map_timed(std::map<std::string, std::string>& map, workload& wl, std::uint32_t operation_count, utils::timing_obj& timing) {
	timing.set_start(std::chrono::high_resolution_clock::now());
	for (std::uint32_t i = 0; i < operation_count; i++) {
		map.find(wl.next_transaction_key());
	}
	timing.set_end(std::chrono::high_resolution_clock::now());
	// std::cout << "Thread Throughput: " << operation_count*1000/timing.get_duration_milliseconds() << std::endl;
}

void do_map_timed_locked(std::map<std::string, std::string>& map, boost::shared_mutex& mutex, workload& wl, std::uint32_t operation_count, utils::timing_obj& timing) {
	boost::shared_lock<boost::shared_mutex> local_shared_lock(mutex, boost::defer_lock);

	timing.set_start(std::chrono::high_resolution_clock::now());
	for (std::uint32_t i = 0; i < operation_count; i++) {
		local_shared_lock.lock();
		map.find(wl.next_transaction_key());
		local_shared_lock.unlock();
	}
	timing.set_end(std::chrono::high_resolution_clock::now());
}

void do_data_gen_timed(workload& wl, std::uint32_t operation_count, utils::timing_obj& timing) {
	timing.set_start(std::chrono::high_resolution_clock::now());
	for (std::uint32_t i = 0; i < operation_count; i++) {
    	const std::string& key = wl.next_transaction_key();
    	std::string value;
	    wl.build_value(value);
	}
	timing.set_end(std::chrono::high_resolution_clock::now());
}

void do_hash_func_timed(dbindex::abstract_hash<std::uint32_t>& hash, std::vector<std::string>& keys, std::uint32_t operation_count, utils::timing_obj& timing) { 
	std::cout << "Inside" << std::endl;
	timing.set_start(std::chrono::high_resolution_clock::now());
	for (std::uint32_t i = 0; i < operation_count; i++) {
	    hash.get_hash(keys[i]);
	}
	timing.set_end(std::chrono::high_resolution_clock::now());
}

/********* End of debugging ***********/

void client::run_build_records(std::uint8_t thread_count) {
	std::thread threads[thread_count];
	workload wls[thread_count];

	// Insertions 	- Initialization of the index
	for(std::uint32_t t = 0; t < thread_count; t++) {
		wls[t].init(wl_p);
		threads[t] = std::thread(do_insertions_concurrent, std::ref(hash_index), std::ref(wls[t]), wls[t].get_record_count());
		utils::stick_thread_to_core(threads[t].native_handle(), (t*2)%32 + ((t*2)/32));
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
		threads[t] = std::thread(do_transactions_concurrent_timed, std::ref(hash_index), std::ref(wls[t]), wls[t].get_operation_count(), std::ref(timings[t]));
		utils::stick_thread_to_core(threads[t].native_handle(), (t*2)%32 + ((t*2)/32));
	}
	for(std::uint32_t t = 0; t < thread_count; t++) {
		threads[t].join();
	}

	return calc_throughput(timings, wls, thread_count);
}


/********* Debugging only, Do delete ***********/

// void do_locks_timed(boost::shared_lock<boost::shared_mutex>& local_shared_lock, std::uint32_t operation_count, utils::timing_obj& timing)
// void do_map_insertions_concurrent(std::map<std::string, std::string>& map, workload& wl, std::uint32_t operation_count)
// void do_map_timed(std::map<std::string, std::string>& map, workload& wl, std::uint32_t operation_count, utils::timing_obj& timing)
// void do_data_gen_timed(workload& wl, std::uint32_t operation_count, utils::timing_obj& timing)

std::uint32_t client::run_locks(std::uint8_t thread_count, std::uint32_t operation_count) {
	std::thread threads[thread_count];
	utils::timing_obj timings[thread_count];
	boost::shared_mutex mutex;
	workload wls[thread_count];

	// Insertions 	- Initialization of the index
	for(std::uint32_t t = 0; t < thread_count; t++) {
		wls[t].init(wl_p);
		threads[t] = std::thread(do_locks_timed, std::ref(mutex), operation_count, std::ref(timings[t]));
		utils::stick_thread_to_core(threads[t].native_handle(), (t*2)%32 + ((t*2)/32));
	}
	for(std::uint32_t t = 0; t < thread_count; t++) {
		threads[t].join();
	}

	return calc_throughput(timings, wls, thread_count);
}

void client::run_build_records_map(std::map<std::string, std::string>& shared_map, std::uint8_t thread_count, std::uint32_t record_count) {
	std::thread threads[thread_count];
	workload wls[thread_count];

	// Insertions 	- Initialization of the index
	for(std::uint32_t t = 0; t < thread_count; t++) {
		wls[t].init(wl_p);
		threads[t] = std::thread(do_map_insertions_concurrent, std::ref(shared_map), std::ref(wls[t]), record_count);
		utils::stick_thread_to_core(threads[t].native_handle(), (t*2)%32 + ((t*2)/32));
	}
	for(std::uint32_t t = 0; t < thread_count; t++) {
		threads[t].join();
	}
}

std::uint32_t client::run_map(std::map<std::string, std::string>& shared_map, std::uint8_t thread_count, std::uint32_t operation_count) {
	std::thread threads[thread_count];
	utils::timing_obj timings[thread_count];
	workload wls[thread_count];
	std::vector<std::vector<std::string>> thread_keys{thread_count, std::vector<std::string>{operation_count}};

	for(std::uint32_t t = 0; t < thread_count; t++) {
		wls[t].init(wl_p);
		for (std::uint32_t i = 0; i < operation_count; i++) {
			thread_keys[t][i] = wls[t].next_transaction_key();
		}
	}
	// Insertions 	- Initialization of the index
	for(std::uint32_t t = 0; t < thread_count; t++) {
		threads[t] = std::thread(do_map_timed, std::ref(shared_map), std::ref(wls[t]), operation_count, std::ref(timings[t]));
		utils::stick_thread_to_core(threads[t].native_handle(), t*2);
	}
	for(std::uint32_t t = 0; t < thread_count; t++) {
		threads[t].join();
	}

	return calc_throughput(timings, wls, thread_count);
}

std::uint32_t client::run_map_locked(std::map<std::string, std::string>& shared_map, std::uint8_t thread_count, std::uint32_t operation_count) {
	std::thread threads[thread_count];
	utils::timing_obj timings[thread_count];
	boost::shared_mutex mutex;
	workload wls[thread_count];

	// Insertions 	- Initialization of the index
	for(std::uint32_t t = 0; t < thread_count; t++) {
		wls[t].init(wl_p);
		threads[t] = std::thread(do_map_timed_locked, std::ref(shared_map), std::ref(mutex), std::ref(wls[t]), operation_count, std::ref(timings[t]));
		utils::stick_thread_to_core(threads[t].native_handle(), (t*2)%32 + ((t*2)/32));
	}
	for(std::uint32_t t = 0; t < thread_count; t++) {
		threads[t].join();
	}

	return calc_throughput(timings, wls, thread_count);
}

std::uint32_t client::run_data_gen(std::uint8_t thread_count, std::uint32_t operation_count) {
	std::thread threads[thread_count];
	utils::timing_obj timings[thread_count];
	workload wls[thread_count];

	// Insertions 	- Initialization of the index
	for(std::uint32_t t = 0; t < thread_count; t++) {
		wls[t].init(wl_p);
		threads[t] = std::thread(do_data_gen_timed, std::ref(wls[t]), operation_count, std::ref(timings[t]));
		utils::stick_thread_to_core(threads[t].native_handle(), (t*2)%32 + ((t*2)/32));
	}
	for(std::uint32_t t = 0; t < thread_count; t++) {
		threads[t].join();
	}

	return calc_throughput(timings, wls, thread_count);
}

std::uint32_t client::run_hash_func(dbindex::abstract_hash<std::uint32_t>& hash, std::uint8_t thread_count, std::uint32_t operation_count) {
	std::thread threads[thread_count];
	utils::timing_obj timings[thread_count];
	workload wls[thread_count];

	std::vector<std::string> keys[thread_count];
	std::cout << "before keys" << std::endl;
	// Insertions 	- Initialization of the index
	for(std::uint32_t t = 0; t < thread_count; t++) {
		wls[t].init(wl_p);
		for(std::uint32_t i = 0; i < operation_count; i++) {
			keys[t].push_back(wls[t].next_transaction_key());
		}
	}
	std::cout << "after keys" << std::endl;
	
	for(std::uint32_t t = 0; t < thread_count; t++) {
		threads[t] = std::thread(do_hash_func_timed, std::ref(hash), std::ref(keys[t]), operation_count, std::ref(timings[t]));
		utils::stick_thread_to_core(threads[t].native_handle(), (t*2)%32 + ((t*2)/32));
	}
	for(std::uint32_t t = 0; t < thread_count; t++) {
		threads[t].join();
	}

	return calc_throughput(timings, wls, thread_count);
}

/********* End of debugging ***********/

std::uint32_t client::run_workload(std::uint8_t thread_count) {
	std::thread threads[thread_count];
	utils::timing_obj timings[thread_count];
	workload wls[thread_count];
	// Insertions 	- Initialization of the index
	for(std::uint32_t t = 0; t < thread_count; t++) {
		wls[t].init(wl_p);
		threads[t] = std::thread(do_insertions_concurrent_timed, std::ref(hash_index), std::ref(wls[t]), wls[t].get_record_count(), std::ref(timings[t]));
		utils::stick_thread_to_core(threads[t].native_handle(), (t*2)%32 + ((t*2)/32));
	}
	for(std::uint32_t t = 0; t < thread_count; t++) {
		threads[t].join();
	}

	// Transactions - Running the designed workload.
	for(std::uint32_t t = 0; t < thread_count; t++) {
		threads[t] = std::thread(do_transactions_concurrent_timed, std::ref(hash_index), std::ref(wls[t]), wls[t].get_operation_count(), std::ref(timings[t]));
		utils::stick_thread_to_core(threads[t].native_handle(), (t*2)%32 + ((t*2)/32));
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
