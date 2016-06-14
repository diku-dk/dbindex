#ifndef push_ops_h
#define push_ops_h

#include <functional>
#include <queue>

class abstract_push_op {
public:
    virtual ~abstract_push_op() {} ;
    virtual bool invoke(const char *keyp, size_t keylen,
            const std::string &value)=0;
};

class concat_push_op : public abstract_push_op {
private:
	std::string concat_result = "";
public:
    ~concat_push_op() {}
    
    bool invoke(const char *keyp, size_t keylen, const std::string &value) {
    	concat_result += ", " + value;
    	return true;
    }
    std::string get() {
    	return concat_result;
    }
};

class vector_push_op : public abstract_push_op {
private:
    std::vector<std::string> result{};
public:
    ~vector_push_op() {}
    
    bool invoke(const char *keyp, size_t keylen, const std::string &value) {
        result.push_back(value);
        return true;
    }

    void clear(){
        result.clear();
    }

    std::vector<std::string> get() {
        return result;
    }
};

class limit_op : public abstract_push_op {
public:
    // ordered_map map<key,value> results;
    std::uint32_t range_scanned = 0;
    size_t len;
    ~limit_op() {}

    bool invoke(const char *keyp, size_t keylen, const std::string &value) {
        // add the key value in the results map
        return (++range_scanned > len) ? false : true;
    }

    void set_len(size_t _len) {
        len = _len;
    }
};



typedef std::tuple<std::string, std::string> hash_entry;

struct hash_entry_comp {};

struct less_than_hash_entry : hash_entry_comp {
    bool operator()(const std::tuple<std::string, std::string>& lhs, 
                    const std::tuple<std::string, std::string>& rhs) {
        return std::get<0>(lhs) > std::get<0>(rhs);
    }
};
struct greater_than_hash_entry : hash_entry_comp {
    bool operator()(const std::tuple<std::string, std::string>& lhs, 
                    const std::tuple<std::string, std::string>& rhs) {
        return std::get<0>(lhs) < std::get<0>(rhs);
    }
};

template <typename cmp>
class partitioned_push_op : public abstract_push_op {
private:
    std::string prefix;
    std::priority_queue<hash_entry, std::vector<hash_entry>, cmp> pri_queue;
public:
    partitioned_push_op(std::string _prefix) : prefix(_prefix){
        static_assert(std::is_base_of<hash_entry_comp, cmp>::value, "hash_entry comparator must inherit from hash_entry_comp");
    }

    ~partitioned_push_op() {}
    
    bool invoke(const char* keyp, size_t keylen, const std::string& value) {
        pri_queue.push(std::make_tuple(prefix + std::string(keyp, keylen), value));
        return true;
    }

    void set_prefix(std::string _prefix) {
        prefix = _prefix;
    }
    std::priority_queue<hash_entry, std::vector<hash_entry>, cmp> get() {
        return pri_queue;
    }
};

#endif