#ifndef baseline_hash_table_h
#define baseline_hash_table_h

#include <iostream>
#include <strings.h>
#include <functional>
#include <algorithm>
#include <queue>
#include <vector>
#include <boost/thread.hpp>

#include "../abstract_index.h"
#include "../macros.h"
#include "../push_ops.h"

typedef std::uint32_t hash_value_t;

namespace dbindex {
    template<std::uint8_t initial_global_depth>
    class baseline_hash_table : public abstract_index {
    private:
        abstract_hash<hash_value_t>& hash;

    public:
        baseline_hash_table(abstract_hash<hash_value_t>& _hash) : hash(_hash) {}
        ~baseline_hash_table() {}

        bool get(const std::string& key, std::string& value) override { return false; }

        void insert(const std::string& key, const std::string& new_value) override {}
        
        void update(const std::string& key, const std::string& new_value) override {}

        void remove(const std::string& key) override {}

        void range_scan(const std::string& start_key, const std::string* end_key, abstract_push_op& apo) override{}

        void reverse_range_scan(const std::string& start_key, const std::string* end_key, abstract_push_op& apo) override{}
    };
}

#endif