#ifndef client_h
#define client_h

#include <string>

#include <boost/thread.hpp>
#include <map> 

#include "../../abstract_index.h"
#include "workload.h"
#include "core_workloads.h"
#include "ycsb_util.h"

namespace ycsb {

class client {
 public:
    client(dbindex::abstract_index& _hash_index, const workload_properties& p, std::uint8_t _thread_count)
     : hash_index(_hash_index), thread_count(_thread_count), wl_p(p) {
    }
    
    std::uint32_t run_workload();
    void run_build_records(std::uint8_t thread_count);
    std::uint32_t run_transactions(std::uint8_t thread_count);

    std::uint32_t run_locks(std::uint8_t thread_count, std::uint32_t operation_count);
    void run_build_records_map(std::map<std::string, std::string>& shared_map, std::uint8_t thread_count, std::uint32_t record_count);
    std::uint32_t run_map(std::map<std::string, std::string>& shared_map, std::uint8_t thread_count, std::uint32_t operation_count);
    std::uint32_t run_map_locked(std::map<std::string, std::string>& shared_map, std::uint8_t thread_count, std::uint32_t operation_count);
    std::uint32_t run_data_gen(std::uint8_t thread_count, std::uint32_t operation_count);


    


    ~client() {}
    
 protected:

    dbindex::abstract_index& hash_index;
    std::uint8_t thread_count;
    const workload_properties wl_p;
};

}

#endif
