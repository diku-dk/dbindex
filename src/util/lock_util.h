#ifndef SRC_UTIL_LOCK_UTIL_H_
#define SRC_UTIL_LOCK_UTIL_H_

#include <chrono>
#include <atomic>
#include <pthread.h>

namespace utils {

class spinlock {
    std::atomic_flag flag = ATOMIC_FLAG_INIT;
public:
    void lock()
    {
        while(flag.test_and_set(std::memory_order_acquire));
    }
    void unlock()
    {
        flag.clear(std::memory_order_release);
    }
};

}

#endif /* SRC_UTIL_LOCK_UTIL_H_ */