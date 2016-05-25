#ifndef scrambled_zipfian_generator_h
#define scrambled_zipfian_generator_h

#include "abstract_generator.h"

#include <cstdint>
#include "zipfian_generator.h"
#include "../ycsb_util.h"

namespace ycsb {

template<typename value_t>
class scrambled_zipfian_generator : public abstract_generator<value_t> {
  public:
    scrambled_zipfian_generator(value_t min, value_t max, double zipfian_const = zipfian_generator<value_t>::k_zipfian_const) :
        base(min), num_items(max - min + 1), zipfian_gen(min, max, zipfian_const) { }
    
    scrambled_zipfian_generator(value_t num_items) : scrambled_zipfian_generator(0, num_items - 1) { }
    
    value_t next();
    value_t last() { return last_value; }
    
  private:
    value_t base;
    value_t num_items;
    zipfian_generator<value_t> zipfian_gen;
    value_t last_value;
};

template <typename value_t>
inline value_t scrambled_zipfian_generator<value_t>::next() {
    value_t value = zipfian_gen.next();
    value = base + utils::fnv_hash_64(value) % num_items;
    return last_value = value;
}

}

#endif
