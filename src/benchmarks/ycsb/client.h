#ifndef client_h
#define client_h

#include <string>

#include "../../abstract_index.h"
#include "workload.h"
#include "core_workloads.h"
#include "ycsb_util.h"

namespace ycsb {

class client {
 public:
    client(dbindex::abstract_index& _hash_index, const workload_properties& p, std::uint8_t _thread_count)
     : hash_index(_hash_index), thread_count(_thread_count) {
        wl.init(p);
    }
    
    std::uint32_t run_workload();
    void run_build_records(std::uint8_t thread_count);
    std::uint32_t run_transactions(std::uint8_t thread_count);

    ~client() {}
    
 protected:

    dbindex::abstract_index& hash_index;
    workload wl;
    std::uint8_t thread_count;
};

}

#endif