#ifndef murmur_hash_h
#define murmur_hash_h

#include <climits>
#include <cstring>
#include <cstdint>
#include <algorithm>
#include <random>
#include <stdexcept>
#include <cassert>
#include "abstract_hash.h"

#define FORCE_INLINE inline __attribute__((always_inline))
#define BIG_CONSTANT(x) (x##LLU)
#define ROTL32(x,y) rotl32(x,y)

inline std::uint32_t rotl32 ( std::uint32_t x, std::int8_t r )
{
  return (x << r) | (x >> (32 - r));
}

FORCE_INLINE std::uint32_t fmix32 ( std::uint32_t h )
{
  h ^= h >> 16;
  h *= 0x85ebca6b;
  h ^= h >> 13;
  h *= 0xc2b2ae35;
  h ^= h >> 16;

  return h;
}

namespace multicore_hash {

  template<typename value_t = std::uint32_t>
  class murmur_hash : public abstract_hash<value_t> {
  private:
    value_t seed;
  public:
    murmur_hash() {
      std::random_device rd;
      std::mt19937 generator(rd());
      std::uniform_int_distribution<value_t> distribution(0, std::numeric_limits<value_t>::max());
      seed = distribution(generator) | 1;
    }    

    value_t get_hash(const std::string key) override {
      static_assert(sizeof(value_t) == sizeof(std::uint32_t), "value_t must be 32 bit");

      const std::uint8_t *ukey    = reinterpret_cast<const std::uint8_t *> (key.c_str());
      const std::uint8_t  len     = key.size(); 
      const std::uint32_t nblocks = len / 4;
      value_t h1 = seed;

      const value_t c1 = 0xcc9e2d51;
      const value_t c2 = 0x1b873593;
    
      //----------
      // body
    
      const std::uint32_t * blocks = (const std::uint32_t *)(ukey + nblocks*4);
    
      for(std::int32_t i = -nblocks; i; i++)
      {
        value_t k1 = blocks[i];
    
        k1 *= c1;
        k1 = ROTL32(k1,15);
        k1 *= c2;
        
        h1 ^= k1;
        h1 = ROTL32(h1,13); 
        h1 = h1*5+0xe6546b64;
      }
    
      //----------
      // tail
    
      const std::uint8_t * tail = (const std::uint8_t*)(ukey + nblocks*4);
    
      value_t k1 = 0;
    
      switch(len & 3)
      {
      case 3: k1 ^= tail[2] << 16;
      case 2: k1 ^= tail[1] << 8;
      case 1: k1 ^= tail[0];
              k1 *= c1; k1 = ROTL32(k1,15); k1 *= c2; h1 ^= k1;
      };
    
      //----------
      // finalization
    
      h1 ^= len;
      h1  = fmix32(h1);
      return h1;
    } 
  };
}
#endif

