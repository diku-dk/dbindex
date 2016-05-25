#ifndef skewed_latest_generator_h
#define skewed_latest_generator_h

#include "abstract_generator.h"

#include <cstdint>
#include "counter_generator.h"
#include "zipfian_generator.h"

namespace ycsb {

template <typename value_t>
class skewed_latest_generator : public abstract_generator<value_t> {
 public:
  skewed_latest_generator(counter_generator<value_t> &counter) :
      basis_gen(counter), zipfian_gen(basis_gen.last()) {
    next();
  }
  
  value_t next();
  value_t last() { return last_value; }
 private:
  counter_generator<value_t> &basis_gen;
  zipfian_generator<value_t> zipfian_gen;
  value_t last_value;
};

template <typename value_t>
inline value_t skewed_latest_generator<value_t>::next() {
  value_t max = basis_gen.last();
  return last_value = max - zipfian_gen.next(max);
}

}

#endif
