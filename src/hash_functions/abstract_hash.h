#ifndef SRC_HASH_FUNCTIONS_ABSTRACT_HASH_H_
#define SRC_HASH_FUNCTIONS_ABSTRACT_HASH_H_

#include <string>
#include <iostream>
#include <stdexcept>

namespace dbindex {
    /**
     * Interface for a hash function
     */
    template<typename value_t>
    class abstract_hash {
    protected:
        virtual void check_key_not_empty(const std::string& key) const final {
            if (key.length() == 0) {
                std::cout << "Empty key" << std::endl;
                throw std::invalid_argument("Key cannot be empty");
            }
        }
    public:
        virtual value_t get_hash(const std::string& key) = 0;
        virtual ~abstract_hash() {
        }
        ;
    };
}

#endif /* SRC_HASH_FUNCTIONS_ABSTRACT_HASH_H_ */
