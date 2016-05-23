#ifndef counter_generator_h
#define counter_generator_h

#include <cstdint>
#include <atomic>
#include "abstract_generator.h"


namespace ycsb {

template<typename value_t>
class counter_generator : public abstract_generator<value_t> {
  public:
    counter_generator(value_t start) : counter(start) { }
    value_t next() { return counter.fetch_add(1); }
    value_t last() { return counter.load() - 1; }
    void set(value_t start) { counter.store(start); }
  private:
    std::atomic<value_t> counter;
};

}

#endif