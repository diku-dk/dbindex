#ifndef client_h
#define client_h

#include <string>

#include <boost/thread.hpp>
#include <map> 

#include "../../abstract_index.h"
#include "workload.h"
#include "core_workloads.h"
#include "ycsb_util.h"
#include "../../util/thread_util.h"

namespace ycsb {

class client {
 public:
    client(dbindex::abstract_index& _hash_index, const workload_properties& p)
     : hash_index(_hash_index), wl_p(p) {
    }
    
    std::uint32_t run_workload(std::uint8_t thread_count);
    void run_build_records(std::uint8_t thread_count);
    std::uint32_t run_transactions(std::uint8_t thread_count);

    std::uint32_t run_locks(std::uint8_t thread_count, std::uint32_t operation_count);
    void run_build_records_map(std::map<std::string, std::string>& shared_map, std::uint8_t thread_count, std::uint32_t record_count);
    std::uint32_t run_map(std::map<std::string, std::string>& shared_map, std::uint8_t thread_count, std::uint32_t operation_count);
    std::uint32_t run_map_locked(std::map<std::string, std::string>& shared_map, std::uint8_t thread_count, std::uint32_t operation_count);
    std::uint32_t run_data_gen(std::uint8_t thread_count, std::uint32_t operation_count);
    std::uint32_t run_hash_func(dbindex::abstract_hash<std::uint32_t>& hash, std::vector<std::vector<std::string>>& keys, std::uint8_t thread_count, std::uint32_t operation_count);
    

    ~client() {}
    
 protected:
    dbindex::abstract_index& hash_index;
    const workload_properties wl_p;
    std::uint32_t calc_throughput(utils::timing_obj timings[], workload wls[], std::uint8_t thread_count);
};

}

#endif
