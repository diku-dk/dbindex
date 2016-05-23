#ifndef SRC_UTIL_THREAD_UTIL_H_
#define SRC_UTIL_THREAD_UTIL_H_

#include <pthread.h>

namespace utils {

static inline int stick_thread_to_core(pthread_t thread, int core_id) {
   	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
	CPU_SET(core_id, &cpuset);
	
	return pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
}

}
#endif /* SRC_UTIL_THREAD_UTIL_H_ */
