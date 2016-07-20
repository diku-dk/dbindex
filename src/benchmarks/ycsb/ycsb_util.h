#ifndef utils_h
#define utils_h

#include <cstdint>
#include <random>
#include <algorithm>
#include <exception>

namespace utils {

const uint64_t k_fnv_offset_basis_64 = 0xCBF29CE484222325;
const uint64_t k_fnv_prime_64 = 1099511628211;

inline uint64_t fnv_hash_64(uint64_t val) {
  uint64_t hash = k_fnv_offset_basis_64;

  for (int i = 0; i < 8; i++) {
    uint64_t octet = val & 0x00ff;
    val = val >> 8;

    hash = hash ^ octet;
    hash = hash * k_fnv_prime_64;
  }
  return hash;
}

}
#endif
