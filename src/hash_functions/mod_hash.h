#ifndef mod_hash_h
#define mod_hash_h

#include <cstdint>

namespace dbindex {
    template<typename value_t = std::uint32_t, std::uint32_t _mod_value = 2>
    class mod_hash: public abstract_hash<value_t> {
    private:
        std::uint32_t mod_value;
    public:
        mod_hash() {
            mod_value = _mod_value;
        }
        value_t get_hash(const std::string& key) override {
            const std::uint32_t ukey = std::atoi(key.c_str());
            // std::cout << "ModHash: " << skey << ", " << ukey << std::endl;
            return ukey % mod_value;
        }
    };
}

#endif
