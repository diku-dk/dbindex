#ifndef extendible_hash_table_h
#define extendible_hash_table_h

#include <iostream>
#include <strings.h>
#include <functional>
#include <algorithm>
#include <queue>
#include <vector>
#include <boost/thread.hpp>

#include "../abstract_index.h"
#include "../macros.h"
#include "../push_ops.h"
#include "../util/lock_util.h"

typedef std::uint32_t hash_value_t;

namespace dbindex {
    template<std::uint8_t initial_global_depth>
    class extendible_hash_table : public abstract_index {
    private:
        abstract_hash<hash_value_t>& hash;
            
        static const std::uint8_t bucket_entries = 4; //(CACHE_LINE_SIZE - sizeof(std::uint8_t))/sizeof(hash_entry);
        struct alignas(CACHE_LINE_SIZE) hash_bucket {
            std::uint8_t  local_depth;
            std::uint8_t  entry_count;
            const std::uint32_t original_index;

            std::vector<std::string> keys;
            std::vector<std::string> values;
            #ifdef _USE_LOCAL_LOCKS
                #ifdef _USE_SPINLOCK
                    utils::spinlock local_lock;
                #else
                    boost::shared_mutex local_mutex; 
                #endif
            #endif

            hash_bucket(std::uint8_t _local_depth, std::uint8_t _bucket_entries, const std::uint32_t _original_index) : local_depth(_local_depth), original_index(_original_index){
                keys = std::vector<std::string>{_bucket_entries};
                values = std::vector<std::string>{_bucket_entries};
                entry_count = 0;
            }

            ~hash_bucket() {}
            void clear(){
                for (std::uint8_t e = 0; e < entry_count; e++) {
                    keys[e] = "";
                    values[e] = "";
                }
                entry_count = 0;
            }
            void set_equal_to_copy(const hash_bucket &other){
                local_depth  = other.local_depth;
                entry_count  = other.entry_count;
                std::copy(&other.keys[0], &other.keys[bucket_entries], &keys[0]);
                std::copy(&other.values[0], &other.values[bucket_entries], &values[0]);
            }
            void insert_next(std::string new_key, std::string new_value) {
                keys  [entry_count] = new_key;
                values[entry_count] = new_value;
                entry_count++;
            }

            void move_last_to(std::uint32_t i) {
                entry_count--;
                keys  [i] = keys[entry_count];
                values[i] = values[entry_count];
                keys  [entry_count] = "";
                values[entry_count] = "";

            }
        };

        std::uint8_t global_depth = initial_global_depth;
        std::vector<hash_bucket*> directory{directory_size()};

        #ifdef _USE_GLOBAL_LOCK
            #ifdef _USE_SPINLOCK
                utils::spinlock global_lock;
            #else
                boost::shared_mutex global_mutex; 
            #endif
        #endif

        std::uint32_t create_bit_mask(std::uint32_t b)
        {
            std::uint32_t r = 0;
            for (std::uint32_t i=0; i<=b; i++)
                r |= 1 << i;
            return r;
        }

        std::vector<hash_bucket*> create_split_buckets(hash_bucket* initial_bucket, std::uint32_t bucket_number, std::string new_key, std::string new_value) {
            std::vector<hash_bucket*> buckets_to_insert;

            hash_bucket* bucket = initial_bucket;
            // std::cout << "Starting : " << hash.get_hash(new_key) << std::endl;
            while (true) { // Runs until broken, i.e. when a spot is found for the new key.
                bucket->local_depth++;
                // if (bucket->local_depth > 7)
                    // print_extendible_hash_bucket(bucket, bucket_number, false);
                bucket->entry_count = 0;

                // Create image bucket //
                std::uint32_t image_number = bucket_number + (1<<(bucket->local_depth-1));
                hash_bucket* image_bucket = new hash_bucket(bucket->local_depth, bucket_entries, image_number);
                buckets_to_insert.push_back(image_bucket);
                
                // Split entries between bucket and image bucket //
                for (uint8_t i = 0; i < bucket_entries; i++) {
                    if (!((hash.get_hash(bucket->keys[i]) >> (bucket->local_depth-1)) & 1)) { // Original bucket
                        bucket->insert_next(bucket->keys[i], bucket->values[i]);
                    }
                    else { // Image bucket
                        image_bucket->insert_next(bucket->keys[i], bucket->values[i]);
                    }
                }

                // Check which bucket new value should enter //
                hash_value_t new_hash_value = hash.get_hash(new_key);
                if (!((new_hash_value >> (bucket->local_depth-1)) & 1)) { // Should go in original bucket
                    if (bucket->entry_count < bucket_entries) { // Insert in bucket
                        bucket->insert_next(new_key, new_value);
                        break;
                    } // Else, continue (left out)
                } // Else, it should go in the image bucket
                else if (image_bucket->entry_count < bucket_entries) { // Insert new in image bucket
                    image_bucket->insert_next(new_key, new_value);
                    break;
                } else { // Do another iteration
                    bucket = image_bucket;
                    bucket_number = image_number;
                    // (Continue next iteration)
                }
                    // print_extendible_hash_bucket(bucket, bucket_number, false);
            }

            return buckets_to_insert;
        }

        std::uint8_t calc_new_local_depth(hash_bucket *bucket, const hash_value_t new_hash_value, std::uint32_t bucket_number) {
            // Calculating hash values
            // std::cout << "G" << std::endl;
            int greatest_msb = 0;
            hash_value_t hash_values[bucket_entries+1];
            for (std::uint8_t e = 0; e < bucket_entries; e++) {
                hash_values[e] = hash.get_hash(bucket->keys[e]);
                greatest_msb = std::min(greatest_msb, __builtin_clz(hash_values[e]));
            }
            // std::cout << "H" << std::endl;
            greatest_msb = sizeof(std::uint32_t)*8 - greatest_msb;
            hash_values[bucket_entries] = new_hash_value;

            // std::cout << "I" << std::endl;
            // Checking common digits
            std::uint8_t new_local_depth = 0;
            std::uint8_t common_digit_nums;
            // std::cout << "J" << std::endl;
            do {
                new_local_depth++;
                common_digit_nums = 0;
                for (std::uint8_t e = 0; e < bucket_entries+1; e++) {
                    common_digit_nums += (hash_values[e] >> (new_local_depth-1)) & 1;
                }
            }
            while (new_local_depth < greatest_msb && (common_digit_nums == 0 || common_digit_nums == bucket_entries+1));
            // std::cout << "K" << std::endl;

            if (new_local_depth == greatest_msb)
                return -1; // Overflow bucket needed

            return new_local_depth;
        }

        void insert_internal_shared(const std::string& key, const std::string& new_value) {
            #ifdef _USE_GLOBAL_LOCK
                #ifdef _USE_SPINLOCK
                    { // Start global lockguard scope
                    boost::lock_guard<utils::spinlock> global_lock_guard(global_lock);
                #else
                    boost::shared_lock<boost::shared_mutex> global_shared_lock(global_mutex); 
                #endif
            #endif
            hash_value_t hash_value = hash.get_hash(key);
            // std::cout << "Key: " << key << ", hash_value: " <<  hash_value << std::endl;
            std::uint32_t bucket_number = directory[hash_value & create_bit_mask(global_depth-1)]->original_index;
            // std::cout << "A: " << bucket_number << ", " << directory_size() << std::endl;

            #ifdef _USE_LOCAL_LOCKS
                #ifdef _USE_SPINLOCK
                    { // Start local lockguard scope
                    boost::lock_guard<utils::spinlock> local_lock_guard(directory[bucket_number]->local_lock);
                #else
                    boost::unique_lock<boost::shared_mutex> local_exclusive_lock(directory[bucket_number]->local_mutex); 
                #endif
            #endif

            if(std::find(directory[bucket_number]->keys.begin(), directory[bucket_number]->keys.end(), key) != directory[bucket_number]->keys.end()) { // Key already present.
                return;
            }
            // std::cout << "B" << std::endl;
            while (directory[hash_value & create_bit_mask(global_depth-1)]->original_index != bucket_number) { // Ensuring correct bucket number 
                #ifdef _USE_LOCAL_LOCKS
                    #ifdef _USE_SPINLOCK
                        local_lock_guard.~lock_guard<utils::spinlock>(); // [?!]
                    #else
                        local_exclusive_lock.unlock(); 
                    #endif
                #endif
                // std::cout << "B1" << std::endl;
                bucket_number = directory[hash_value & create_bit_mask(global_depth-1)]->original_index;
                // std::cout << "B2" << std::endl;
                #ifdef _USE_LOCAL_LOCKS
                    #ifdef _USE_SPINLOCK
                        boost::lock_guard<utils::spinlock> local_lock_guard(directory[bucket_number]->local_lock);
                    #else
                        local_exclusive_lock = boost::unique_lock<boost::shared_mutex>(directory[bucket_number]->local_mutex); 
                    #endif
                #endif
                // std::cout << "B3" << std::endl;

            }
            // std::cout << "C" << std::endl;

            if (directory[bucket_number]->entry_count < bucket_entries) {
                directory[bucket_number]->insert_next(key, new_value);
                return;
            } 
            std::uint8_t new_local_depth = calc_new_local_depth(directory[bucket_number], hash_value, bucket_number);
            if (new_local_depth <= global_depth) {
                // std::cout << "D" << std::endl;
                std::vector<hash_bucket*> buckets_to_insert = create_split_buckets(directory[bucket_number], bucket_number, key, new_value);

                for (typename std::vector<hash_bucket*>::iterator it = buckets_to_insert.begin(); it != buckets_to_insert.end(); ++it) {
                    hash_bucket*  image_bucket   = *it;
                    std::uint32_t ptr_index      = image_bucket->original_index;
                    while (ptr_index < directory_size()) { // Update all other pointers with this prefix.
                        directory[ptr_index] = image_bucket;
                        ptr_index += (1<<image_bucket->local_depth);
                    }
                }
                return;
            } else if (new_local_depth == 255) {
                // std::cout << "E" << std::endl;
                // std::cout << "new_local_depth " << (std::int32_t)new_local_depth << std::endl;
                throw "Overflow Bucket";
            }
            // std::cout << "F" << std::endl;
            #ifdef _USE_LOCAL_LOCKS
                #ifdef _USE_SPINLOCK
                    } // end local lockguard scope
                    // local_lock_guard.unlock();
                #else
                    local_exclusive_lock.unlock(); 
                #endif
            #endif
            #ifdef _USE_GLOBAL_LOCK
                #ifdef _USE_SPINLOCK
                    } // end global lockguard scope
                    // global_lock_guard.unlock();
                #else
                    global_shared_lock.unlock(); 
                #endif
            #endif
            insert_internal_exclusive(key, new_value);
        }
        void insert_internal_exclusive(const std::string& key, const std::string& new_value) {
            // std::cout << "EXCL" << std::endl;
            #ifdef _USE_GLOBAL_LOCK
                #ifdef _USE_SPINLOCK
                    boost::lock_guard<utils::spinlock> global_lock_guard(global_lock);
                #else
                    boost::unique_lock<boost::shared_mutex> global_exclusive_lock(global_mutex); 
                #endif
            #endif

            // Search for free slot
            hash_value_t hash_value = hash.get_hash(key);
            std::uint32_t bucket_number = directory[hash_value & create_bit_mask(global_depth-1)]->original_index;

            if (directory[bucket_number]->entry_count < bucket_entries) {
                directory[bucket_number]->insert_next(key, new_value);
                return;
            } 

            std::vector<hash_bucket*> buckets_to_insert = create_split_buckets(directory[bucket_number], bucket_number, key, new_value);

            if (buckets_to_insert.back()->local_depth <= global_depth) {
                for (typename std::vector<hash_bucket*>::iterator it = buckets_to_insert.begin(); it != buckets_to_insert.end(); ++it) {
                    hash_bucket*  image_bucket   = *it;
                    std::uint32_t ptr_index      = image_bucket->original_index;
                    while (ptr_index < directory_size()) { // Update all other pointers with this prefix.
                        directory[ptr_index] = image_bucket;
                        ptr_index += (1<<image_bucket->local_depth);
                    }
                }
                return;
            }

            std::uint8_t splits = buckets_to_insert.back()->local_depth - global_depth;
            size_t old_size = 1 << global_depth;
            size_t new_size = 1 << (global_depth + splits);

            directory.resize(new_size);
            for (typename std::vector<hash_bucket*>::iterator it = buckets_to_insert.begin(); it != buckets_to_insert.end(); ++it) {
                hash_bucket* image_bucket = *it;
                if (image_bucket->original_index > directory_size()-1) {
                    std::copy_n(directory.begin(), old_size, directory.begin()+old_size);
                    old_size <<= 1;
                }
                directory[image_bucket->original_index] = image_bucket;
            }
            global_depth += splits;
        }

    public:
        extendible_hash_table(abstract_hash<hash_value_t>& _hash) : hash(_hash) {
            for (std::uint32_t b = 0; b < directory_size(); b++) {
                hash_bucket *bucket = new hash_bucket(global_depth, bucket_entries, b);
                directory[b] = bucket;
            }
        }
        ~extendible_hash_table() {
            // print_extendible_hash_table(false);
            for (std::uint32_t b = 0; b < directory_size(); b++){
                if (directory[b]) {
                    if (directory[b]->original_index != b)
                        directory[b] = nullptr;
                }
            }
            for (std::uint32_t b = 0; b < directory_size(); b++){
                if (directory[b]) {
                    delete directory[b];
		    directory[b] = nullptr;
                }
            }
        }

        void print_extendible_hash_table(bool exclusive) {
            std::cout << (int)global_depth << std::endl;
            for (std::uint8_t j = 0; j < bucket_entries; j++)
                std::cout << "_______";
            std::cout << "_______" << "_______" << "_______" << "_______" << std::endl;
            for (std::uint32_t i = 0; i < directory_size(); i++) {
                print_extendible_hash_bucket(directory[i], i, exclusive);
            }
            for (std::uint8_t j = 0; j < bucket_entries; j++)
                std::cout << "_______";
            std::cout << "_______" << "_______" << "_______" << "_______" << std::endl;
        }
        void print_extendible_hash_bucket(hash_bucket* bucket, std::uint32_t i, bool exclusive) {
            std::string text;
            text += ((bucket->original_index == i) ? "\033[1m " : "\033[0m "); 
            text += (i < 10 ? "   " : (i < 100 ? "  " : (i < 1000 ? " " : ""))) + std::to_string((int)i) + " | ";
            text += "\033[0m";
            text += ((bucket->original_index == i) ? "\033[1;31m" : "\033[0;31m"); 
            text += (bucket->original_index < 10 ? "   " : (bucket->original_index < 100 ? "  " : (bucket->original_index < 1000 ? " " : ""))) + std::to_string((int)bucket->original_index);
            text += "\033[0m";
            text += " | ";
            text += ((bucket->original_index == i) ? "\033[1;32m" : "\033[0;32m"); 
            text += (bucket->local_depth < 10 ? "   " : (bucket->local_depth < 100 ? "  " : (bucket->local_depth < 1000 ? " " : ""))) + std::to_string((int)bucket->local_depth);
            text += "\033[0m";
            text += " | ";
            text += ((bucket->original_index == i) ? "\033[1;33m" : "\033[0;33m"); 
            text += (bucket->entry_count < 10 ? "   " : (bucket->entry_count < 100 ? "  " : (bucket->entry_count < 1000 ? " " : ""))) + std::to_string((int)bucket->entry_count);
            text += ((bucket->original_index == i) ? "\033[1;34m" : "\033[0m"); 
            text += " | ";
            for (std::uint8_t j = 0; j < bucket_entries; j++)
            {
                if (!exclusive || bucket->original_index == i){
                    if (j >= bucket->entry_count) {
                        text += "     | ";
                    }
                    else {
                        hash.get_hash(bucket->keys[j]);
                        text += ((bucket->keys[j].length() < 2 ? "   " : (bucket->keys[j].length() < 3 ? "  " : (bucket->keys[j].length() < 4 ? " " : ""))) + bucket->keys[j] + ": " + std::to_string(hash.get_hash(bucket->keys[j]))) + " | ";
                    }
                }
                else 
                    text += "     | ";
            }
            text += "\033[0m";
            std::cout << text << std::endl;
        }

        bool get(const std::string& key, std::string& value) override {
            hash_value_t hash_value = hash.get_hash(key);
            
            // Take global lock shared; 
            #ifdef _USE_GLOBAL_LOCK
                #ifdef _USE_SPINLOCK
                    boost::lock_guard<utils::spinlock> global_lock_guard(global_lock);
                #else
                    boost::shared_lock<boost::shared_mutex> global_shared_lock(global_mutex); 
                #endif
            #endif
            std::uint32_t bucket_number = hash_value & (directory_size()-1);

            // Take local lock shared; 
            #ifdef _USE_LOCAL_LOCKS
                #ifdef _USE_SPINLOCK
                    boost::lock_guard<utils::spinlock> local_lock_guard(directory[bucket_number]->local_lock);
                #else
                    boost::shared_lock<boost::shared_mutex> local_shared_lock(directory[bucket_number]->local_mutex); 
                #endif
            #endif
            for (uint8_t i = 0; i < bucket_entries; i++) {
                if (directory[bucket_number]->keys[i] == key) {
                    value = directory[bucket_number]->values[i];
                    return true;
                }
            }
            return false;
        }

        void insert(const std::string& key, const std::string& new_value) override {
            insert_internal_shared(key, new_value);
        }
        
        void update(const std::string& key, const std::string& new_value) override {
            hash_value_t hash_value = hash.get_hash(key);

            #ifdef _USE_GLOBAL_LOCK
                #ifdef _USE_SPINLOCK
                    boost::lock_guard<utils::spinlock> global_lock_guard(global_lock);
                #else
                    boost::shared_lock<boost::shared_mutex> global_shared_lock(global_mutex); 
                #endif
            #endif
            std::uint32_t bucket_number = hash_value & (directory_size()-1);
            
            #ifdef _USE_LOCAL_LOCKS
                #ifdef _USE_SPINLOCK
                    boost::lock_guard<utils::spinlock> local_lock_guard(directory[bucket_number]->local_lock);
                #else
                    boost::unique_lock<boost::shared_mutex> local_exclusive_lock(directory[bucket_number]->local_mutex); 
                #endif
            #endif
            for (uint8_t i = 0; i < bucket_entries; i++) {
                if (directory[bucket_number]->keys[i] == key) {
                    directory[bucket_number]->values[i] = new_value;
                    break;
                }
            }
        }

        void remove(const std::string& key) override {
            hash_value_t hash_value = hash.get_hash(key);
            #ifdef _USE_GLOBAL_LOCK
                #ifdef _USE_SPINLOCK
                    boost::lock_guard<utils::spinlock> global_lock_guard(global_lock);
                #else
                    boost::shared_lock<boost::shared_mutex> global_shared_lock(global_mutex); 
                #endif
            #endif
            std::uint32_t bucket_number = hash_value & (directory_size()-1);
            #ifdef _USE_LOCAL_LOCKS
                #ifdef _USE_SPINLOCK
                    boost::lock_guard<utils::spinlock> local_lock_guard(directory[bucket_number]->local_lock);
                #else
                    boost::unique_lock<boost::shared_mutex> local_exclusive_lock(directory[bucket_number]->local_mutex); 
                #endif
            #endif
            for (uint8_t i = 0; i < directory[bucket_number]->entry_count; i++) {
                if (directory[bucket_number]->keys[i] == key) { // Entry to be updated found
                    directory[bucket_number]->move_last_to(i);
                    break;
                }
            }
        }

        void range_scan(const std::string& start_key, const std::string* end_key, abstract_push_op& apo) override{
            if (end_key) assert(*end_key > start_key);

            typedef std::tuple<std::string, std::string> hash_entry;

            auto cmp = [](hash_entry a, hash_entry b) { return std::get<0>(a) > std::get<0>(b);};
            std::priority_queue<hash_entry, std::vector<hash_entry>, decltype(cmp)> pri_queue(cmp);

            // FULL SCAN
            #ifdef _USE_GLOBAL_LOCK
                #ifdef _USE_SPINLOCK
                    boost::lock_guard<utils::spinlock> global_lock_guard(global_lock);
                #else
                    boost::shared_lock<boost::shared_mutex> global_shared_lock(global_mutex); 
                #endif
            #endif
            for (std::uint32_t i = 0; i < directory_size(); i++) {          
                for (std::uint8_t j = 0; j < directory[i]->entry_count; j++) {
                    if (directory[i]->keys[j] >= start_key && (!end_key || directory[i]->keys[j] <= *end_key)) {
                        pri_queue.push(std::make_tuple(directory[i]->keys[j], directory[i]->values[j]));
                    }
                }
            }
            // Apply push op
            while(!pri_queue.empty()) {
                hash_entry current = pri_queue.top();
                std::string key     = std::get<0>(current);
                std::string value = std::get<1>(current);
                const char* keyp = key.c_str();
                if (!apo.invoke(keyp, key.length(), value)) {
                    return;
                }
                pri_queue.pop();
            }
        }

        void reverse_range_scan(const std::string& start_key, const std::string* end_key, abstract_push_op& apo) override{
            if (end_key) assert(*end_key > start_key);
            
            typedef std::tuple<std::string, std::string> hash_entry;

            auto cmp = [](hash_entry a, hash_entry b) { return std::get<0>(a) < std::get<0>(b);};
            std::priority_queue<hash_entry, std::vector<hash_entry>, decltype(cmp)> pri_queue(cmp);

            // FULL SCAN
            #ifdef _USE_GLOBAL_LOCK
                #ifdef _USE_SPINLOCK
                    boost::lock_guard<utils::spinlock> global_lock_guard(global_lock);
                #else
                    boost::shared_lock<boost::shared_mutex> global_shared_lock(global_mutex); 
                #endif
            #endif
            for (std::uint32_t i = 0; i < directory_size(); i++) {          
                for (std::uint8_t j = 0; j < directory[i]->entry_count; j++) {
                    if (directory[i]->keys[j] >= start_key && (!end_key || directory[i]->keys[j] <= *end_key)) {
                        pri_queue.push(std::make_tuple(directory[i]->keys[j], directory[i]->values[j]));
                    }
                }
            }
            // Apply push op
            while(!pri_queue.empty()) {
                hash_entry current = pri_queue.top();
                std::string key     = std::get<0>(current);
                std::string value = std::get<1>(current);
                const char* keyp = key.c_str();
                if (!apo.invoke(keyp, key.length(), value)) {
                    return;
                }
                pri_queue.pop();
            }
        }

        std::uint8_t get_global_depth() {
            return global_depth;
        }
        std::uint8_t get_bucket_entries() {
            return bucket_entries;
        }

        size_t directory_size() {
            return (1<<global_depth);
        }
        size_t size() {
            #ifdef _USE_GLOBAL_LOCK
                #ifdef _USE_SPINLOCK
                    boost::lock_guard<utils::spinlock> global_lock_guard(global_lock);
                #else
                    boost::shared_lock<boost::shared_mutex> global_shared_lock(global_mutex); 
                #endif
            #endif
            size_t total_entry_count = 0;
            for (std::uint32_t i = 0; i < directory_size(); i++) {
                if (directory[i]->original_index == i) {
                    total_entry_count += directory[i]->entry_count;
                }
            }
            return total_entry_count;
        }
    };
}

#endif