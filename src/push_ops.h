#ifndef push_ops_h
#define push_ops_h

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

#endif