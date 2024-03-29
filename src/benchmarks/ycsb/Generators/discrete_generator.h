#ifndef discrete_generator_h
#define discrete_generator_h

#include "abstract_generator.h"

#include <vector>
#include <cassert>
#include "../ycsb_util.h"

namespace ycsb {

template<typename value_t>
class discrete_generator : public abstract_generator<value_t> {
  public:
    discrete_generator() : sum(0) { }
    value_t next();
    value_t last() { return last_value; }
    void add_option(value_t value, double weight);
  private:
    std::vector<std::pair<value_t, double>> values;
    double sum;
    value_t last_value;
};

template <typename value_t>
inline void discrete_generator<value_t>::add_option(value_t value, double weight) {
    if (values.empty()) {
        last_value = value;
    }
    values.push_back(std::make_pair(value, weight));
    sum += weight;
}

template <typename value_t>
inline value_t discrete_generator<value_t>::next() {
    double chooser = utils::random_double();
    
    for (std::pair<value_t, double> p : values) {
        if (chooser < p.second / sum) {
            return last_value = p.first;
        }
        chooser -= p.second / sum;
    }
    
    assert(false);
    return last_value;
}

} 

#endif
