#ifndef core_workloads_h
#define core_workloads_h

#include <cstddef>

namespace ycsb {

enum operation_type {
    INSERT,
    READ,
    UPDATE,
    SCAN,
    READMODIFYWRITE
};

enum distribution_type {
    ZIPFIAN,
    UNIFORM,
    LATEST,
    UNUSED
};

struct workload_properties {
    size_t record_count;
    size_t operation_count;
    
    double read_proportion;
    double update_proportion;
    double scan_proportion;
    double insert_proportion;
    double read_modify_write_proportion;
    
    distribution_type request_dist;
    distribution_type scan_len_dist;
    
    size_t max_scan_len;
    size_t max_value_len;
};

// extern const workload_properties workload_a;

// extern const workload_properties workload_b;

// extern const workload_properties workload_c;

// extern const workload_properties workload_d;

// extern const workload_properties workload_e;

// extern const workload_properties workload_f;

}

#endif