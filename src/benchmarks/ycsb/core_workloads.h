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

    bool operator==(const workload_properties& other) {
    return  record_count                 == other.record_count
         && operation_count              == other.operation_count
         && read_proportion              == other.read_proportion
         && update_proportion            == other.update_proportion
         && scan_proportion              == other.scan_proportion
         && insert_proportion            == other.insert_proportion
         && read_modify_write_proportion == other.read_modify_write_proportion
         && request_dist                 == other.request_dist
         && scan_len_dist                == other.scan_len_dist
         && max_scan_len                 == other.max_scan_len
         && max_value_len                == other.max_value_len;
    }
};

// Counts:        record, operation
// Proportions:   read, update, scan, insert, RMW
// Distributions: request, scan_len
// Max_values:    scan_len, value_len
static const workload_properties workload_a = {
    100000, 100000, 
    0.5, 0.5, 0, 0, 0, 
    distribution_type::ZIPFIAN, distribution_type::UNUSED, 
    0, 100
};
static const workload_properties workload_b = {
    100000, 100000, 
    0.95, 0.05, 0, 0, 0, 
    distribution_type::ZIPFIAN, distribution_type::UNUSED, 
    0, 100
};
static const workload_properties workload_c = {
    100000, 100000, 
    1, 0, 0, 0, 0, 
    distribution_type::ZIPFIAN, distribution_type::UNUSED, 
    0, 100
};
static const workload_properties workload_d = {
    100000, 100000, 
    0.95, 0.05, 0, 0, 0, 
    distribution_type::LATEST, distribution_type::UNUSED, 
    0, 100
};
static const workload_properties workload_e = {
    100000, 100000, 
    0, 0, 0.95, 0.05, 0, 
    distribution_type::ZIPFIAN, distribution_type::UNIFORM, 
    100, 100
};
static const workload_properties workload_f = {
    100000, 100000,
    0.5, 0, 0, 0, 0.5, 
    distribution_type::ZIPFIAN, distribution_type::UNUSED, 
    0, 100
};


// Counts:        record, operation
// Proportions:   read, update, scan, insert, RMW
// Distributions: request, scan_len
// Max_values:    scan_len, value_len
static const workload_properties workload_read = {
    100000, 100000,
    1, 0, 0, 0, 0, 
    distribution_type::ZIPFIAN, distribution_type::UNUSED, 
    0, 100
};

static const workload_properties workload_update = {
    100000, 100000,
    0, 1, 0, 0, 0, 
    distribution_type::ZIPFIAN, distribution_type::UNUSED, 
    0, 100
};
static const workload_properties workload_scan = {
    10000, 100000,
    0, 0, 1, 0, 0, 
    distribution_type::ZIPFIAN, distribution_type::UNUSED, 
    0, 100
};
static const workload_properties workload_insert = {
    100000, 100000,
    0, 0, 0, 1, 0, 
    distribution_type::ZIPFIAN, distribution_type::UNUSED, 
    0, 100
};
static const workload_properties workload_read_modify_write = {
    100000, 100000,
    0, 0, 0, 0, 1, 
    distribution_type::ZIPFIAN, distribution_type::UNUSED, 
    0, 100
};

}

#endif
