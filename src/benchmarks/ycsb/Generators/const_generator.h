#ifndef YCSB_C_CONST_GENERATOR_H_
#define YCSB_C_CONST_GENERATOR_H_

#include "abstract_generator.h"
#include <cstdint>

namespace ycsb {

template <typename value_t>
class const_generator : public abstract_generator<value_t> {
 public:
  const_generator(int constant) : constant_(constant) { }
  value_t next() { return constant_; }
  value_t last() { return constant_; }
 private:
  value_t constant_;
};

} // ycsb

#endif // YCSB_C_CONST_GENERATOR_H_
