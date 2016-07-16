#ifndef SRC_UTIL_LOCK_UTIL_H_
#define SRC_UTIL_LOCK_UTIL_H_

#include <chrono>
#include <atomic>
#include <pthread.h>
#include <boost/thread.hpp>

namespace utils {

class spinlock {
    std::atomic_flag flag = ATOMIC_FLAG_INIT;
    boost::mutex mutex;
public:
    void lock()
    {
        while(flag.test_and_set(std::memory_order_acq_rel));
        mutex.lock();

    }
    void unlock()
    {
    	mutex.unlock();
        flag.clear(std::memory_order_release);
    }
};

}

#endif /* SRC_UTIL_LOCK_UTIL_H_ */