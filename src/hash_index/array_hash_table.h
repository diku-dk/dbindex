#ifndef array_hash_table_h
#define array_hash_table_h

#include <iostream>
#include <functional>
#include <queue>
#include <vector>
#include <boost/thread.hpp>
#include "abstract_hash_table.h"

#define CACHE_LINE_SIZE 64
typedef boost::shared_mutex shared_mutex;
typedef std::string value_t;
namespace multicore_hash {

	template<typename key_t, typename test, std::uint32_t directory_size>
	class array_hash_table : public abstract_hash_table<key_t, value_t> {
	private:
		abstract_hash<hash_value_t> *hash;
			
		struct alignas(CACHE_LINE_SIZE) hash_bucket {

			std::vector<key_t> keys;
			std::vector<value_t> values;

			hash_bucket(std::uint32_t _initial_bucket_size) {
				keys   = std::vector<key_t  >(_initial_bucket_size);
				values = std::vector<value_t>(_initial_bucket_size);
			}
			hash_bucket(key_t key, value_t value) {
				keys        = std::vector<key_t  >(1, key);
				values      = std::vector<value_t>(1, value);
			}

			~hash_bucket() {
			}

			void insert_next(key_t new_key, value_t new_value) {
				keys.push_back(new_key);
				values.push_back(new_value);
			}

			void move_last_to(std::uint32_t i) {
				keys  [i] = keys[keys.size()-1];
				values[i] = values[values.size()-1];
				keys.pop_back();
				values.pop_back();
			}
		};

		hash_bucket **directory;
		shared_mutex *bucket_mutexes;

	public:
		array_hash_table(abstract_hash<hash_value_t> *_hash, std::uint32_t initial_bucket_size = 2) : hash(_hash) {
			directory = (hash_bucket**) malloc(directory_size*sizeof(hash_bucket*));
			for (std::uint32_t b = 0; b < directory_size; b++) {
				directory[b] = NULL;
			}
			bucket_mutexes = new shared_mutex[directory_size];
		}
			
		~array_hash_table() {
			for (std::uint32_t b = 0; b < directory_size; b++) {
				if (directory[b]) {
					// Deallocate the memory
					delete directory[b];
					// void actual pointer
					directory[b] = NULL;
				}
			}
			free(directory);
			delete[] bucket_mutexes;
		}
		std::string align_int_1000(std::uint32_t input) { 
			return (input < 10 ? "   " : (input < 100 ? "  " : (input < 1000 ? " " : ""))) + std::to_string(input);
		}
		std::string align_str_1000(std::string input_str) { 
			std::uint32_t input = atoi(input_str.c_str());
			return (input < 10 ? "   " : (input < 100 ? "  " : (input < 1000 ? " " : ""))) + std::to_string(input);
		}

		void print_array_hash_table() {
			std::string text;
			std::cout << std::string(7*(directory_size+1), '_') << std::endl; // Print first horizontal line
			bool keep_printing = true;
			std::uint32_t c = 0;
			while (keep_printing) { // As long as at least one bucket has more to print.
				keep_printing = false; // Assume everyone is done.
				text = " " + align_int_1000(c) + " | ";
				for (std::uint8_t i = 0; i < directory_size; i++) {
					if (directory[i] && directory[i]->keys.size() > c) { // Print if there is more in the current bucket
						text += align_str_1000(directory[i]->keys[c]) + " | ";
						if (directory[i]->keys.size() > c+1) // Check if the bucket is done
							keep_printing = true; // Otherwise, signal to keep printing
					} else {
						text += "     | ";
					}
				}
				std::cout << text << std::endl;
				c++;
			}
			std::cout << std::string(7*(directory_size+1), '_') << std::endl;
		}

		bool get(const key_t& key, value_t& value) override {
			hash_value_t hash_value = hash->get_hash(key);
			std::uint32_t bucket_number = hash_value & (directory_size-1);

			boost::shared_lock<shared_mutex> local_shared_lock(bucket_mutexes[bucket_number]);
			if (directory[bucket_number]) {
				for (uint32_t i = 0; i < directory[bucket_number]->keys.size(); i++) {
					if (directory[bucket_number]->keys[i] == key) {
						value = directory[bucket_number]->values[i];
						local_shared_lock.unlock();
						return true;
					}
				}
			}
			local_shared_lock.unlock();
			return false;
		}

		void insert(const key_t& key, const value_t& new_value) override {
			hash_value_t hash_value = hash->get_hash(key);
			std::uint32_t bucket_number = hash_value & (directory_size-1);

			boost::unique_lock<shared_mutex> local_exclusive_lock(bucket_mutexes[bucket_number]);
			if (directory[bucket_number]) {
				directory[bucket_number]->insert_next(key, new_value);
			} else {
				hash_bucket* bucket = new hash_bucket(key, new_value);
				directory[bucket_number] = bucket;
			}
			local_exclusive_lock.unlock();

		}
		// Returns previous value, if found, -1 otherwise
		void update(const key_t& key, const value_t& new_value) override {
			hash_value_t hash_value = hash->get_hash(key);
			std::uint32_t bucket_number = hash_value & (directory_size-1);
			
			boost::unique_lock<shared_mutex> local_exclusive_lock(bucket_mutexes[bucket_number]);
			if (directory[bucket_number]) {
				for (uint32_t i = 0; i < directory[bucket_number]->keys.size(); i++) {
					if (directory[bucket_number]->keys[i] == key) {
						directory[bucket_number]->values[i] = new_value;
						break;
					}
				}
			}
			local_exclusive_lock.unlock();
		}

		// Returns deleted value, if found, -1 otherwise
		void remove(const key_t& key) override {
			hash_value_t hash_value = hash->get_hash(key);
			std::uint32_t bucket_number = hash_value & (directory_size-1);
			boost::unique_lock<shared_mutex> local_exclusive_lock(bucket_mutexes[bucket_number]);
			if (directory[bucket_number]) {
				for (uint32_t i = 0; i < directory[bucket_number]->keys.size(); i++) {
					if (directory[bucket_number]->keys[i] == key) { // Entry to be removed found
						directory[bucket_number]->move_last_to(i);
						break;
					}
				}
			}
			local_exclusive_lock.unlock();
		}

		void range_scan(const key_t& start_key, const key_t* end_key, abstract_push_op& apo) override{
			typedef std::tuple<key_t, value_t> hash_entry;

			auto cmp = [](hash_entry a, hash_entry b) { return std::get<0>(a) > std::get<0>(b);};
			std::priority_queue<hash_entry, std::vector<hash_entry>, decltype(cmp)> pri_queue(cmp);

			// FULL SCAN
			for (std::uint32_t i = 0; i < directory_size; i++) {
				boost::shared_lock<shared_mutex> local_shared_lock(bucket_mutexes[i]);
				for (std::uint32_t j = 0; j < directory[i]->keys.size(); j++) {
					if (directory[i]->keys[j] >= start_key && directory[i]->keys[j] <= *end_key) {
						pri_queue.push(std::make_tuple(directory[i]->keys[j], directory[i]->values[j]));
					}
				}
				local_shared_lock.unlock();
			}
			// Apply push op
			while(!pri_queue.empty()) {
				hash_entry current = pri_queue.top();
				key_t key     = std::get<0>(current);
				value_t value = std::get<1>(current);
				const char* keyp = key.c_str();
				if (!apo.invoke(keyp, key.length(), value)) {
					return;
				}
				pri_queue.pop();
			}
		}

		void reverse_range_scan(const key_t& start_key, const key_t* end_key, abstract_push_op& apo) override{
			typedef std::tuple<key_t, value_t> hash_entry;

			auto cmp = [](hash_entry a, hash_entry b) { return std::get<0>(a) < std::get<0>(b);};
			std::priority_queue<hash_entry, std::vector<hash_entry>, decltype(cmp)> pri_queue(cmp);

			// FULL SCAN
			for (std::uint32_t i = 0; i < directory_size; i++) {			
				boost::shared_lock<shared_mutex> local_shared_lock(bucket_mutexes[i]);
				for (std::uint32_t j = 0; j < directory[i]->keys.size(); j++) {
					if (directory[i]->keys[j] >= start_key && directory[i]->keys[j] <= *end_key) {
						pri_queue.push(std::make_tuple(directory[i]->keys[j], directory[i]->values[j]));
					}
				}
				local_shared_lock.unlock();
			}
			// Apply push op
			while(!pri_queue.empty()) {
				hash_entry current = pri_queue.top();
				key_t key     = std::get<0>(current);
				value_t value = std::get<1>(current);
				const char* keyp = key.c_str();
				if (!apo.invoke(keyp, key.length(), value)) {
					return;
				}
				pri_queue.pop();
			}
		}

		hash_bucket **get_directory() {
			return directory;
		}

		std::uint32_t get_directory_size() {
			return directory_size;
		}
		size_t size() {
			size_t total_entry_count = 0;
			for (std::uint32_t i = 0; i < directory_size; i++) {
				if (directory[i]) {
					total_entry_count += directory[i]->keys.size();
				}
			}
			return total_entry_count;
		}
	};
}

#endif