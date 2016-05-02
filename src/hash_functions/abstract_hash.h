#ifndef abstract_hash_h
#define abstract_hash_h

#include <string>

namespace dbindex {
    /**
     * Interface for a hash function
     */
    template<typename value_t>
    class abstract_hash {
    public:
        virtual value_t get_hash(const std::string& key) = 0;
        virtual ~abstract_hash() {
        }
        ;
    };
}

#endif
