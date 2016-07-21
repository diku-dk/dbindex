#ifndef workload_h
#define workload_h

#include <vector>
#include <string>
#include <iostream>

#include "Generators/uniform_generator.h"
#include "Generators/abstract_generator.h"
#include "Generators/discrete_generator.h"
#include "Generators/counter_generator.h"

#include "../../abstract_index.h"
#include "../../push_ops.h"
#include "core_workloads.h"
#include "ycsb_util.h"

typedef std::uint32_t hash_value_t;

namespace ycsb {

class workload {
  public:
    void init(const workload_properties& wl_p);
    
    void do_insert(dbindex::abstract_index& hash_index);
    void do_transaction(dbindex::abstract_index& hash_index);

    std::uint32_t get_record_count()    { return record_count; }
    std::uint32_t get_operation_count() { return operation_count; }
    
    workload() :
        value_len_generator(nullptr), key_generator(nullptr), key_selector(nullptr),
        scan_len_selector(nullptr), insert_sequence_generator(0) {}
    
    ~workload() {
        if (value_len_generator) delete value_len_generator;
        if (key_generator)       delete key_generator;
        if (key_selector)        delete key_selector;
        if (scan_len_selector)   delete scan_len_selector;
    }
    
  protected:
    size_t max_value_len;
    std::uint32_t record_count;
    std::uint32_t operation_count;
    std::string value;

    std::uint32_t rand_char_seed;

    abstract_generator<hash_value_t>  *value_len_generator;       // Uniform(1, max_value_len)
    abstract_generator<hash_value_t>  *key_generator;             // Counter generator

    discrete_generator<operation_type> operation_selector;        // Discrete generator
    abstract_generator<hash_value_t>  *key_selector;              // Uniform/zipfian/latest generator
    abstract_generator<hash_value_t>  *scan_len_selector;         // Uniform/zipfian generator 
    counter_generator<hash_value_t>    insert_sequence_generator; // Counter generator

    limit_op limit_push_op;

    void do_transaction_read(dbindex::abstract_index& hash_index);
    void do_transaction_insert(dbindex::abstract_index& hash_index);
    void do_transaction_update(dbindex::abstract_index& hash_index);
    void do_transaction_range_scan(dbindex::abstract_index& hash_index);
    void do_transaction_read_modify_write(dbindex::abstract_index& hash_index);

    void build_value(std::string& value);
    
    std::string next_sequence_key(); 
    std::string next_transaction_key(); 
    operation_type next_operation()     { return operation_selector.next(); }
    size_t next_scan_length()           { return scan_len_selector->next(); }
};

inline std::string workload::next_sequence_key() {
    #ifdef _TEST_PREFIX_BITS
        value = "";
        // Ensure prefix bits in front.
        for (hash_value_t c = 0; c < 4; c++) {
            value += (rand_r(&rand_char_seed) % 255) + 1;
        }
        return value + std::to_string(key_generator->next());
    #else
        return std::to_string(key_generator->next());
    #endif
}

inline std::string workload::next_transaction_key() {
    hash_value_t key;
    do {
        key = key_selector->next();
    } while (key > insert_sequence_generator.last());

    #ifdef _TEST_PREFIX_BITS
        value = "";
        // Ensure prefix bits in front.
        for (hash_value_t c = 0; c < 4; c++) {
            value += (rand_r(&rand_char_seed) % 255) + 1;
        }
        return value + std::to_string(key);
    #else
        return std::to_string(key);
    #endif
}


inline void workload::do_insert(dbindex::abstract_index& hash_index) {
    std::string key = next_sequence_key();

    build_value(value);
    hash_index.insert(key, value);
}


inline void workload::do_transaction(dbindex::abstract_index& hash_index) {
    switch (next_operation()) {
        case READ:
            do_transaction_read(hash_index);
            return;
        case UPDATE:
            do_transaction_update(hash_index);
            return;
        case INSERT:
            do_transaction_insert(hash_index);
            return;
        case SCAN:
            do_transaction_range_scan(hash_index);
            return;
        case READMODIFYWRITE:
            do_transaction_read_modify_write(hash_index);
            return;
        default:
            throw std::invalid_argument("Unknown operation.!");
    }
}

inline void workload::do_transaction_read(dbindex::abstract_index& hash_index) {
    const std::string& key = next_transaction_key();

    hash_index.get(key, value);
}


inline void workload::do_transaction_insert(dbindex::abstract_index& hash_index) {
    const std::string& key = std::to_string(key_selector->next());

    build_value(value);
    hash_index.insert(key, value);
} 

inline void workload::do_transaction_read_modify_write(dbindex::abstract_index& hash_index) {
    const std::string& key = next_transaction_key();

    hash_index.get(key, value);
    build_value(value);
    hash_index.update(key, value);
}

inline void workload::do_transaction_update(dbindex::abstract_index& hash_index) {
    const std::string& key = next_transaction_key();

    build_value(value);
    hash_index.update(key, value);
}

inline void workload::do_transaction_range_scan(dbindex::abstract_index& hash_index) {
    const std::string& start_key = next_transaction_key();

    limit_push_op.set_len(next_scan_length());
    
    hash_index.range_scan(start_key, nullptr, limit_push_op);
}

}

#endif
