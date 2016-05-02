#ifndef mult_shift_hash_h
#define mult_shift_hash_h

#include <climits>
#include <cstring>
#include <cstdint>
#include <algorithm>
#include <random>
#include <stdexcept>
#include <cassert>
#include <iostream>
#include "abstract_hash.h"


namespace dbindex {
  template<typename value_t = std::uint32_t>
  class mult_shift_hash : public abstract_hash<value_t> {
  private:
    const std::uint8_t l = sizeof(value_t)*8;
    value_t seed;
    
  public:
    mult_shift_hash() {
      std::random_device rd;
      std::mt19937 generator(rd());
      std::uniform_int_distribution<value_t> distribution(0, std::numeric_limits<value_t>::max());
      seed = distribution(generator) | 1;
    }    

    value_t get_hash(const std::string& key) override {
      uint64_t ukey; 
      std::copy(&key[0], &key[0] + sizeof(ukey), reinterpret_cast<char*>(&ukey));
      return (value_t)((seed*ukey) >> (64-l));
    }
  };
}
#endif
