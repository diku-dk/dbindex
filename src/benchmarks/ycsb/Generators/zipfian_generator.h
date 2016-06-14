#ifndef zipfian_generator_h
#define zipfian_generator_h

#include <cstdint>
#include <cmath>
#include <cassert>
#include <climits>
#include "abstract_generator.h"
#include "../ycsb_util.h"

namespace ycsb {

template <typename value_t>
class zipfian_generator : public abstract_generator<value_t> {
 public:
    static constexpr const double k_zipfian_const = 0.99;
    static const value_t k_max_num_items = std::numeric_limits<value_t>::max();
    
    zipfian_generator(value_t min, value_t max, double zipfian_const = k_zipfian_const) :
            num_items(max - min + 1), base(min), theta(zipfian_const), zeta_n(0), n_for_zeta(0) {

        assert(num_items >= 2 && num_items < k_max_num_items);
        zeta_2_ = zeta(2, theta);
        alpha_ = 1.0 / (1.0 - theta);
        raise_zeta(num_items);
        eta_ = eta();
        
        next();
    }
    
    zipfian_generator(value_t num_items) : zipfian_generator(0, num_items - 1, k_zipfian_const) { }
    
    value_t next(value_t num_items);
    
    value_t next() { return next(num_items); }

    value_t last() { return last_value; }
    
 private:
    ///
    /// Compute the zeta constant needed for the distribution.
    /// Remember the number of items, so if it is changed, we can recompute zeta.
    ///
    void raise_zeta(value_t num) {
        assert(num >= n_for_zeta);
        zeta_n = zeta(n_for_zeta, num, theta, zeta_n);
        n_for_zeta = num;
    }
    
    double eta() {
        return (1 - std::pow(2.0 / num_items, 1 - theta)) /
                (1 - zeta_2_ / zeta_n);
    }

    ///
    /// Calculate the zeta constant needed for a distribution.
    /// Do this incrementally from the last_num of items to the cur_num.
    /// Use the zipfian constant as theta. Remember the new number of items
    /// so that, if it is changed, we can recompute zeta.
    ///
    static double zeta(value_t last_num, value_t cur_num,
                                         double theta, double last_zeta) {
        double zeta = last_zeta;
        for (uint64_t i = last_num + 1; i <= cur_num; ++i) {
            zeta += 1 / std::pow(i, theta);
        }
        return zeta;
    }
    
    static double zeta(value_t num, double theta) {
        return zeta(0, num, theta, 0);
    }
    
    value_t num_items;
    value_t base; /// Min number of items to generate
    
    // Computed parameters for generating the distribution
    double theta, zeta_n, eta_, alpha_, zeta_2_;
    value_t n_for_zeta; /// Number of items used to compute zeta_n
    value_t last_value;
};

template <typename value_t>
inline value_t zipfian_generator<value_t>::next(value_t num) {
    assert(num >= 2 && num < k_max_num_items);
    if (num > n_for_zeta) { // Recompute zeta_n and eta
        raise_zeta(num);
        eta_ = eta();
    }
    
    double u = utils::random_double();
    double uz = u * zeta_n;
    
    if (uz < 1.0) {
        return last_value = 0;
    }
    
    if (uz < 1.0 + std::pow(0.5, theta)) {
        return last_value = 1;
    }

    return last_value = base + num * std::pow(eta_ * u - eta_ + 1, alpha_);
}

}
#endif