#ifndef abstract_hash_table_h
#define abstract_hash_table_h

#include "../hash_functions/abstract_hash.h"
#include <string>

namespace multicore_hash {

	class abstract_push_op {
    public:
        virtual ~abstract_push_op() {
        }
        ;
        virtual bool invoke(const char *keyp, size_t keylen,
                const std::string &value)=0;
    };

	template<typename key_t, typename value_t>
	class abstract_hash_table
	{
	private:
		abstract_hash<key_t> *hash;
	public:
		virtual bool get   (const key_t& key, value_t& value) = 0;
        virtual void update(const key_t& key, const value_t& value) = 0;
        virtual void insert(const key_t& key, const value_t& value) = 0;
        virtual void remove(const key_t& key) = 0;
        virtual void range_scan(const key_t& start_key,
                				const key_t* end_key, abstract_push_op&) = 0;
        virtual void reverse_range_scan(const key_t& start_key,
                						const key_t* end_key, abstract_push_op&) = 0;
        // virtual std::size_t size() const = 0;
	};
}

#endif
