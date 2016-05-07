#ifndef extendible_hash_table_h
#define extendible_hash_table_h

#include <iostream>
#include <functional>
#include <queue>
#include <vector>
#include <boost/thread.hpp>

#include "../abstract_index.h"

#define CACHE_LINE_SIZE 64
typedef boost::shared_mutex shared_mutex;
typedef std::uint32_t hash_value_t;

namespace dbindex {
	template<typename key_t, typename value_t, std::uint8_t initial_global_depth>
	class extendible_hash_table : public abstract_index {
	private:
		abstract_hash<hash_value_t> *hash;
			
		static const std::uint8_t bucket_entries = 4; //(CACHE_LINE_SIZE - sizeof(std::uint8_t))/sizeof(hash_entry);
		struct alignas(CACHE_LINE_SIZE) hash_bucket {
			std::uint8_t  local_depth;
			std::uint8_t  entry_count;

			key_t *keys;
			value_t *values;
			shared_mutex local_mutex;

			hash_bucket(std::uint8_t _local_depth, std::uint8_t _bucket_entries) : local_depth(_local_depth){
				keys = new key_t[_bucket_entries];
				values = new value_t[_bucket_entries];
				entry_count = 0;
			}
			hash_bucket(const hash_bucket &other) : local_depth(other.local_depth){
				keys = new key_t[bucket_entries];
				values = new value_t[bucket_entries];
				std::copy(&other.keys[0], &other.keys[bucket_entries], &keys[0]);
				std::copy(&other.values[0], &other.values[bucket_entries], &values[0]);
				entry_count = other.entry_count;
			}

			~hash_bucket() {
				delete[] keys;
				delete[] values;
			}
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
			void insert_next(key_t new_key, value_t new_value) {
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
		hash_bucket **directory;

		shared_mutex global_mutex;

		std::uint32_t createBitMask(std::uint32_t b)
		{
			std::uint32_t r = 0;
			for (std::uint32_t i=0; i<=b; i++)
				r |= 1 << i;
			return r;
		}

		std::uint32_t calc_original_index(std::uint32_t bucket_number) {
			return bucket_number & createBitMask(directory[bucket_number]->local_depth);
		}

		void create_bucket_and_insert(hash_bucket *bucket, std::vector<std::pair<std::uint32_t, hash_bucket*>> &buckets_to_insert, std::uint32_t bucket_number, key_t key, value_t value) {
			// Redistribute entries from bucket
			bucket->local_depth++;
			bucket->entry_count = 0;
			std::uint32_t image_number = bucket_number + (1<<(bucket->local_depth-1));
			hash_bucket *image_bucket = new hash_bucket(bucket->local_depth, bucket_entries);
			for (uint8_t i = 0; i < bucket_entries; i++) {
				if (!((hash->get_hash(bucket->keys[i]) >> (bucket->local_depth-1)) & 1)) { // Original bucket
					bucket->insert_next(bucket->keys[i], bucket->values[i]);
				}
				else { // Image bucket
					image_bucket->insert_next(bucket->keys[i], bucket->values[i]);
				}
			}

			// Insert new value
			hash_value_t new_hash_value = hash->get_hash(key);
			if ((!((new_hash_value >> (bucket->local_depth-1)) & 1)) && bucket->entry_count < bucket_entries) { // Insert new in original bucket
				bucket->insert_next(key, value);
			} else if (((new_hash_value >> (bucket->local_depth-1)) & 1) && image_bucket->entry_count < bucket_entries) { // Insert new in image bucket
				image_bucket->insert_next(key, value);
			}
			buckets_to_insert.push_back(std::make_pair(image_number, image_bucket));
		}

	public:
		extendible_hash_table(abstract_hash<hash_value_t> *_hash) : hash(_hash) {
			directory = (hash_bucket**) malloc(directory_size()*sizeof(hash_bucket*));
			for (std::uint8_t b = 0; b < directory_size(); b++) {
				hash_bucket *bucket = new hash_bucket(global_depth, bucket_entries);
				directory[b] = bucket;
			}
		}
		~extendible_hash_table() {
			boost::unique_lock<shared_mutex> global_exclusive_lock(global_mutex);
			for (std::uint32_t b = 0; b < directory_size(); b++){
				if (directory[b]) {
					// Deallocate the memory
					if (directory[b]->local_depth == global_depth) { // Delete if only one pointer is pointing.
						delete directory[b];
					} else { 
						std::uint32_t mask = (createBitMask(global_depth-directory[b]->local_depth-1)<<(directory[b]->local_depth));
						if ((mask & b) == mask) { // Delete if is last pointer to bucket
							delete directory[b];
						}
					}
					
					// void actual pointer
					directory[b] = NULL;
				}
			}
			free(directory);
			global_exclusive_lock.unlock();
		}

		void print_extendible_hash_table(bool exclusive) {
			for (std::uint8_t j = 0; j < bucket_entries; j++)
				std::cout << "_______";
			std::cout << "_______" << "_______" << "_______" << "_______" << std::endl;
			for (std::uint32_t i = 0; i < directory_size(); i++)
				print_extendible_hash_bucket(directory[i], i, exclusive);
			for (std::uint8_t j = 0; j < bucket_entries; j++)
				std::cout << "_______";
			std::cout << "_______" << "_______" << "_______" << "_______" << std::endl;
		}
		void print_extendible_hash_bucket(hash_bucket* bucket, std::uint32_t i, bool exclusive) {
			std::string text;
			std::uint32_t origin_index = calc_original_index(i, bucket->local_depth);
			text += ((origin_index == i) ? "\033[1m " : "\033[0m "); 
			text += (i < 10 ? "   " : (i < 100 ? "  " : (i < 1000 ? " " : ""))) + std::to_string((int)i) + " | ";
			text += "\033[0m";
			text += ((origin_index == i) ? "\033[1;31m" : "\033[0;31m"); 
			text += (origin_index < 10 ? "   " : (origin_index < 100 ? "  " : (origin_index < 1000 ? " " : ""))) + std::to_string((int)origin_index);
			text += "\033[0m";
			text += " | ";
			text += ((origin_index == i) ? "\033[1;32m" : "\033[0;32m"); 
			text += (bucket->local_depth < 10 ? "   " : (bucket->local_depth < 100 ? "  " : (bucket->local_depth < 1000 ? " " : ""))) + std::to_string((int)bucket->local_depth);
			text += "\033[0m";
			text += " | ";
			text += ((origin_index == i) ? "\033[1;33m" : "\033[0;33m"); 
			text += (bucket->entry_count < 10 ? "   " : (bucket->entry_count < 100 ? "  " : (bucket->entry_count < 1000 ? " " : ""))) + std::to_string((int)bucket->entry_count);
			text += ((origin_index == i) ? "\033[1;34m" : "\033[0m"); 
			text += " | ";
			for (std::uint8_t j = 0; j < bucket_entries; j++)
			{
				if (!exclusive || origin_index == i){
					std::uint32_t value = atoi(bucket->keys[j].c_str());
					if (j >= bucket->entry_count)
						text += "     | ";
					else
						text += ((value < 10 ? "   " : (value < 100 ? "  " : (value < 1000 ? " " : ""))) + bucket->keys[j]) + " | ";
				}
				else 
					text += "     | ";
			}
			text += "\033[0m";
			std::cout << text << std::endl;
		}

		bool get(const key_t& key, value_t& value) override {
			hash_value_t hash_value = hash->get_hash(key);
			
			// Take global lock shared;
			boost::shared_lock<shared_mutex> global_shared_lock(global_mutex);
			std::uint32_t bucket_number = hash_value & (directory_size()-1);

			// Take local lock shared;
			boost::shared_lock<shared_mutex> local_shared_lock(directory[bucket_number]->local_mutex);
			for (uint8_t i = 0; i < bucket_entries; i++) {
				if (directory[bucket_number]->keys[i] == key) {
					value = directory[bucket_number]->values[i];
					local_shared_lock.unlock();
					global_shared_lock.unlock();
					return true;
				}
			}
			local_shared_lock.unlock();
			global_shared_lock.unlock();
			return false;
		}

		void insert(const key_t& key, const value_t& new_value) override {

			boost::shared_lock<shared_mutex> global_shared_lock(global_mutex);

			// Search for free slot
			hash_value_t hash_value = hash->get_hash(key);

			std::uint32_t bucket_number = calc_original_index(hash_value & createBitMask(global_depth-1));

			boost::unique_lock<shared_mutex> local_exclusive_lock(directory[bucket_number]->local_mutex, boost::defer_lock);
			if (!local_exclusive_lock.try_lock()) {
				global_shared_lock.unlock();
				return insert(key, new_value);
			}
			if (directory[bucket_number]->entry_count < bucket_entries) {
				directory[bucket_number]->insert_next(key, new_value);
				local_exclusive_lock.unlock();
				global_shared_lock.unlock();
				return;
			}

			// hash_bucket *tmp_bucket = new hash_bucket(*directory[bucket_number]);
			typedef std::pair<std::uint32_t, hash_bucket*> entry;
			std::vector<entry> buckets_to_insert;

			//      Not made                      Original bucket overflow                    Image bucket overflow
			while (buckets_to_insert.empty() || !(std::get<1>(buckets_to_insert.back())->entry_count) || !(directory[bucket_number]->entry_count)) {
				if (buckets_to_insert.empty() || !(std::get<1>(buckets_to_insert.back())->entry_count)) {
					create_bucket_and_insert(directory[bucket_number], buckets_to_insert, bucket_number, key, new_value);
				} else {
					create_bucket_and_insert(std::get<1>(buckets_to_insert.back()), buckets_to_insert, bucket_number, key, new_value);
				}
			}

			global_shared_lock.unlock();
			boost::unique_lock<shared_mutex> global_exclusive_lock(global_mutex);
			// Check if directory doesn't need to be doubled
			if (std::get<1>(buckets_to_insert.back())->local_depth <= global_depth) {
				auto image_bucket = std::get<1>(buckets_to_insert.back()); // Image bucket
				auto tmp_index 	  = std::get<0>(buckets_to_insert.back()); // Place to put bucket
				while (tmp_index < directory_size()) { // Update all other pointers with this prefix.
					directory[tmp_index] = image_bucket;
					tmp_index += (1<<image_bucket->local_depth);
				}
				local_exclusive_lock.unlock();
				global_exclusive_lock.unlock();
				return;
			}	

			std::uint8_t splits = std::get<1>(buckets_to_insert.back())->local_depth - global_depth;

			std::uint32_t old_size = (1<<global_depth);
			std::uint32_t new_size = 1 << (global_depth + splits);

			directory = (hash_bucket**) realloc(directory, new_size*sizeof(hash_bucket*));
			
			if (!directory) {
				std::cout << "REALLOC ERROR" << std::endl;
			}		
			for (typename std::vector<std::pair<std::uint32_t, hash_bucket*>>::iterator it = buckets_to_insert.begin(); it != buckets_to_insert.end(); ++it) {
				std::copy(&directory[0], &directory[old_size], &directory[old_size]);
				old_size <<= 1;
				hash_bucket* image_bucket   = std::get<1>(*it);
				directory[std::get<0>(*it)] = image_bucket;
			}
			global_depth += splits;

			local_exclusive_lock.unlock();
			global_exclusive_lock.unlock();
		}
		// Returns previous value, if found, -1 otherwise
		void update(const key_t& key, const value_t& new_value) override {
			hash_value_t hash_value = hash->get_hash(key);

			boost::shared_lock<shared_mutex> global_shared_lock(global_mutex);
			std::uint32_t bucket_number = hash_value & (directory_size()-1);
			
			boost::unique_lock<shared_mutex> local_exclusive_lock(directory[bucket_number]->local_mutex);
			for (uint8_t i = 0; i < bucket_entries; i++) {
				if (directory[bucket_number]->keys[i] == key) {
					directory[bucket_number]->values[i] = new_value;
					break;
				}
			}
			local_exclusive_lock.unlock();
			global_shared_lock.unlock();
		}

		// Returns deleted value, if found, -1 otherwise
		void remove(const key_t& key) override {
			hash_value_t hash_value = hash->get_hash(key);
			boost::shared_lock<shared_mutex> global_shared_lock(global_mutex);
			std::uint32_t bucket_number = hash_value & (directory_size()-1);
			boost::unique_lock<shared_mutex> local_exclusive_lock(directory[bucket_number]->local_mutex);
			for (uint8_t i = 0; i < directory[bucket_number]->entry_count; i++) {
				if (directory[bucket_number]->keys[i] == key) { // Entry to be updated found
					directory[bucket_number]->move_last_to(i);
					break;
				}
			}
			local_exclusive_lock.unlock();
			global_shared_lock.unlock();
		}

		void range_scan(const key_t& start_key, const key_t* end_key, abstract_push_op& apo) override{
			typedef std::tuple<key_t, value_t> hash_entry;

			auto cmp = [](hash_entry a, hash_entry b) { return std::get<0>(a) > std::get<0>(b);};
			std::priority_queue<hash_entry, std::vector<hash_entry>, decltype(cmp)> pri_queue(cmp);

			// FULL SCAN
			boost::shared_lock<shared_mutex> global_shared_lock(global_mutex);
			for (std::uint32_t i = 0; i < directory_size(); i++) {			
				for (std::uint8_t j = 0; j < directory[i]->entry_count; j++) {
					if (directory[i]->keys[j] >= start_key && directory[i]->keys[j] <= *end_key)
						pri_queue.push(std::make_tuple(directory[i]->keys[j], directory[i]->values[j]));
				}
			}
			// Apply push op
			while(!pri_queue.empty()) {
				hash_entry current = pri_queue.top();
				key_t key     = std::get<0>(current);
				value_t value = std::get<1>(current);
				const char* keyp = key.c_str();
				if (!apo.invoke(keyp, key.length(), value)) {
					global_shared_lock.unlock();
					return;
				}
				pri_queue.pop();
			}
			global_shared_lock.unlock();
		}

		void reverse_range_scan(const key_t& start_key, const key_t* end_key, abstract_push_op& apo) override{
			typedef std::tuple<key_t, value_t> hash_entry;

			auto cmp = [](hash_entry a, hash_entry b) { return std::get<0>(a) < std::get<0>(b);};
			std::priority_queue<hash_entry, std::vector<hash_entry>, decltype(cmp)> pri_queue(cmp);

			// FULL SCAN
			boost::shared_lock<shared_mutex> global_shared_lock(global_mutex);
			for (std::uint32_t i = 0; i < directory_size(); i++) {			
				for (std::uint8_t j = 0; j < directory[i]->entry_count; j++) {
					if (directory[i]->keys[j] >= start_key && directory[i]->keys[j] <= *end_key)
						pri_queue.push(std::make_tuple(directory[i]->keys[j], directory[i]->values[j]));
				}
			}
			// Apply push op
			while(!pri_queue.empty()) {
				hash_entry current = pri_queue.top();
				key_t key     = std::get<0>(current);
				value_t value = std::get<1>(current);
				const char* keyp = key.c_str();
				if (!apo.invoke(keyp, key.length(), value)) {
					global_shared_lock.unlock();
					return;
				}
				pri_queue.pop();
			}
			global_shared_lock.unlock();
		}

		std::uint8_t get_global_depth() {
			return global_depth;
		}
		std::uint8_t get_bucket_entries() {
			return bucket_entries;
		}
		hash_bucket **get_directory() {
			return directory;
		}

		size_t directory_size() {
			return (1<<global_depth);
		}
		size_t size() {
			boost::shared_lock<shared_mutex> global_shared_lock(global_mutex);
			size_t total_entry_count = 0;
			for (std::uint32_t i = 0; i < directory_size(); i++) {
				if (calc_original_index(i) == i) {
					total_entry_count += directory[i]->entry_count;
				}
			}
			global_shared_lock.unlock();
			return total_entry_count;
		}
	};
}

#endif
