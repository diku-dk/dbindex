#ifndef workload_h
#define workload_h

#include <vector>
#include <string>
#include <iostream>

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
    void init(const workload_properties& p);
    
    void do_insert(dbindex::abstract_index& hash_index);
    void do_transaction(dbindex::abstract_index& hash_index);

    void build_value(std::string& value);
    void build_max_value(std::string& value);
    
    std::string next_sequence_key(); 
    std::string next_transaction_key(); 
    operation_type next_operation()     { return operation_selector.next(); }
    size_t next_scan_length()           { return scan_len_selector->next(); }
    std::uint32_t get_record_count()    { return record_count; }
    std::uint32_t get_operation_count() { return operation_count; }
    
    workload() :
        value_len_generator(NULL), key_generator(NULL), key_selector(NULL),
        scan_len_selector(NULL), insert_sequence_generator(0) {}
    
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

    abstract_generator<hash_value_t>  *value_len_generator;
    abstract_generator<hash_value_t>  *key_generator;

    discrete_generator<operation_type> operation_selector;
    abstract_generator<hash_value_t>  *key_selector;
    abstract_generator<hash_value_t>  *scan_len_selector;
    counter_generator<hash_value_t>    insert_sequence_generator;

    limit_op limit_push_op;

    void do_transaction_read(dbindex::abstract_index& hash_index);
    void do_transaction_read_modify_write(dbindex::abstract_index& hash_index);
    void do_transaction_range_scan(dbindex::abstract_index& hash_index);
    void do_transaction_update(dbindex::abstract_index& hash_index);
    void do_transaction_insert(dbindex::abstract_index& hash_index);
};

inline std::string workload::next_sequence_key() {
    return std::to_string(key_generator->next());
}

inline std::string workload::next_transaction_key() {
    hash_value_t key;
    do {
        key = key_selector->next();
    } while (key > insert_sequence_generator.last());
    return std::to_string(key);
}


inline void workload::do_insert(dbindex::abstract_index& hash_index) {
    // std::cout << "start" << std::endl;
    std::string key = next_sequence_key();
    // std::cout << "key found" << std::endl;
    // std::cout << "Key: " << key << std::endl;
    std::string value;
    

    // std::cout << "building value" << std::endl;
    build_value(value);
    // std::cout << "value built" << std::endl;
    // std::cout << "Value: " << value << std::endl;
    hash_index.insert(key, value);
    // std::cout << "inserted" << std::endl;
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
    std::string value;

    hash_index.get(key, value);
}


inline void workload::do_transaction_insert(dbindex::abstract_index& hash_index) {
    const std::string& key = std::to_string(key_selector->next());
    std::string value;

    build_value(value);
    hash_index.insert(key, value);
} 

inline void workload::do_transaction_read_modify_write(dbindex::abstract_index& hash_index) {
    const std::string& key = next_transaction_key();
    std::string value;
    
    hash_index.get(key, value);
    build_value(value);
    hash_index.update(key, value);
}

inline void workload::do_transaction_update(dbindex::abstract_index& hash_index) {
    const std::string& key = next_transaction_key();
    std::string value;

    build_value(value);
    hash_index.update(key, value);
}

inline void workload::do_transaction_range_scan(dbindex::abstract_index& hash_index) {
    const std::string& start_key = next_transaction_key();
    std::string end_key;
    build_max_value(end_key);

    limit_push_op.set_len(next_scan_length());
    
    hash_index.range_scan(start_key, &end_key, limit_push_op);
}

}

#endif
