#ifndef uniform_generator_h
#define uniform_generator_h

#include "abstract_generator.h"
#include <random>

namespace ycsb {

template<typename value_t>
class uniform_generator : public abstract_generator<value_t> {
  public:
    // Both min and max are inclusive
    uniform_generator(value_t min, value_t max) : dist(min, max) { next(); }
    
    value_t next() { return last_int = dist(generator); }
    value_t last() { return last_int; }
    
  private:
    value_t last_int;
    std::mt19937_64 generator;
    std::uniform_int_distribution<value_t> dist;
};

}

#endif
