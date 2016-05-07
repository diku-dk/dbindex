#ifndef abstract_hash_table_h
#define abstract_hash_table_h

#include "./hash_functions/abstract_hash.h"
#include <string>

namespace dbindex {
    /**
     * Interface for a push operator for
     * a scan operation on the index structure
     */
    class abstract_push_op {
    public:
        virtual ~abstract_push_op() {
        }
        ;
        virtual bool invoke(const char *keyp, size_t keylen,
                const std::string &value) =0;
    };

    /**
     * Interface of the index structure, the scan operation may or
     * may not be implemented
     */
    class abstract_index {
    public:
        virtual bool get(const std::string& key, std::string& value) = 0;
        virtual void update(const std::string& key, const std::string& value) = 0;
        virtual void insert(const std::string& key, const std::string& value) = 0;
        virtual void remove(const std::string& key) = 0;
        virtual void range_scan(const std::string& start_key, const std::string* end_key,
                abstract_push_op&) = 0;
        virtual void reverse_range_scan(const std::string& start_key,
                const std::string* end_key, abstract_push_op&) = 0;
        virtual ~abstract_index() {
        }
        ;
    };
}

#endif
