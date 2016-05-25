#ifndef SRC_UTIL_THREAD_UTIL_H_
#define SRC_UTIL_THREAD_UTIL_H_

#include <chrono>
#include <pthread.h>

namespace utils {

static inline int stick_thread_to_core(pthread_t thread, int core_id) {
   	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
	CPU_SET(core_id, &cpuset);
	
	return pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
}

struct timing_obj {
	std::chrono::_V2::system_clock::time_point start;
	std::chrono::_V2::system_clock::time_point end;

	void set_start(std::chrono::_V2::system_clock::time_point _start) {
		start = _start;
	}

	void set_end(std::chrono::_V2::system_clock::time_point _end) {
		end = _end;
	}

	long get_duration_milliseconds() {
		return std::chrono::duration_cast<std::chrono::milliseconds>(end-start).count();
	}
	
	long get_duration_nanoseconds() {
		return std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count();
	}
};

}
#endif /* SRC_UTIL_THREAD_UTIL_H_ */
