#ifndef abstract_generator_h
#define abstract_generator_h

#include <cstdint>
#include <string>

namespace ycsb {

template<typename value_t>
class abstract_generator {
 public:
  virtual value_t next() = 0;
  virtual value_t last() = 0;
  virtual ~abstract_generator() { }
};

}

#endif