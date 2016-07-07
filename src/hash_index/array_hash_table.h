#ifndef array_hash_table_h
#define array_hash_table_h

#include <iostream>
#include <functional>
#include <queue>
#include <cstring>
#include <vector>
#include <boost/thread.hpp>

#include "../macros.h"

#include "../abstract_index.h"
#include "../push_ops.h"

typedef std::uint32_t hash_value_t;

namespace dbindex {

	template<std::uint32_t directory_size>
	class array_hash_table : public abstract_index {
	private:
		abstract_hash<hash_value_t>& hash;
			
		struct alignas(CACHE_LINE_SIZE) hash_bucket {

			std::vector<std::string> keys;
			std::vector<std::string> values;

			hash_bucket(const std::string& key, const std::string& value) {
				keys   = std::vector<std::string>(1, key);
				values = std::vector<std::string>(1, value);
			}

			~hash_bucket() {
			}

			void insert_next(const std::string& new_key, const std::string& new_value) {
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

		std::vector<hash_bucket*> directory{directory_size};
		std::vector<boost::shared_mutex> bucket_mutexes{directory_size};

	public:
		array_hash_table(abstract_hash<hash_value_t>& _hash) : hash(_hash) {
			for (std::uint32_t b = 0; b < directory_size; b++) {
				directory[b] = nullptr;
			}
		}
		array_hash_table( array_hash_table&& ) = default; // Move constructor
			
		~array_hash_table() {
			for (std::uint32_t b = 0; b < directory_size; b++) {
				if (directory[b]) {
					// Deallocate the memory
					delete directory[b];
					// void actual pointer
					directory[b] = nullptr;
				}
			}
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

		bool get(const std::string& key, std::string& value) override {
			hash_value_t hash_value = hash.get_hash(key);
			std::uint32_t bucket_number = hash_value & (directory_size-1);

			#if defined(_USE_LOCAL_LOCKS) || defined(_USE_LOCAL_INSERT_LOCKS)
			boost::shared_lock<boost::shared_mutex> local_shared_lock(bucket_mutexes[bucket_number]);
			#endif
			if (directory[bucket_number]) {
				for (uint32_t i = 0; i < directory[bucket_number]->keys.size(); i++) {
					if (strcmp(directory[bucket_number]->keys[i].c_str(), key.c_str()) == 0) {
						value = directory[bucket_number]->values[i];
						#ifdef _USE_LOCAL_LOCKS
						local_shared_lock.unlock();
						#endif
						return true;
					}
				}
			}
			#if defined(_USE_LOCAL_LOCKS) || defined(_USE_LOCAL_INSERT_LOCKS)
			local_shared_lock.unlock();
			#endif
			return false;
		}

		void insert(const std::string& key, const std::string& new_value) override {
			hash_value_t hash_value = hash.get_hash(key);
			std::uint32_t bucket_number = hash_value & (directory_size-1);

			#ifdef _USE_LOCAL_LOCKS
			boost::unique_lock<boost::shared_mutex> local_exclusive_lock(bucket_mutexes[bucket_number]);
			#endif
			if (directory[bucket_number]) {
				directory[bucket_number]->insert_next(key, new_value);
			} else {
				hash_bucket* bucket = new hash_bucket(key, new_value);
				directory[bucket_number] = bucket;
			}
			#ifdef _USE_LOCAL_LOCKS
			local_exclusive_lock.unlock();
			#endif

		}

		void update(const std::string& key, const std::string& new_value) override {
			hash_value_t hash_value = hash.get_hash(key);
			std::uint32_t bucket_number = hash_value & (directory_size-1);
			
			#ifdef _USE_LOCAL_LOCKS
			boost::unique_lock<boost::shared_mutex> local_exclusive_lock(bucket_mutexes[bucket_number]);
			#endif
			if (directory[bucket_number]) {
				for (uint32_t i = 0; i < directory[bucket_number]->keys.size(); i++) {
					if (strcmp(directory[bucket_number]->keys[i].c_str(), key.c_str()) == 0) {
						directory[bucket_number]->values[i] = new_value;
						break;
					}
				}
			}
			#ifdef _USE_LOCAL_LOCKS
			local_exclusive_lock.unlock();
			#endif
		}

		void remove(const std::string& key) override {
			hash_value_t hash_value = hash.get_hash(key);
			std::uint32_t bucket_number = hash_value & (directory_size-1);
			#ifdef _USE_LOCAL_LOCKS
			boost::unique_lock<boost::shared_mutex> local_exclusive_lock(bucket_mutexes[bucket_number]);
			#endif
			if (directory[bucket_number]) {
				for (uint32_t i = 0; i < directory[bucket_number]->keys.size(); i++) {
					if (strcmp(directory[bucket_number]->keys[i].c_str(), key.c_str()) == 0) { // Entry to be removed found
						directory[bucket_number]->move_last_to(i);
						break;
					}
				}
			}
			#ifdef _USE_LOCAL_LOCKS
			local_exclusive_lock.unlock();
			#endif
		}

		void range_scan(const std::string& start_key, const std::string* end_key, abstract_push_op& apo) override{
			if (end_key) assert(*end_key > start_key);
			
			// std::cout << "i1" << std::endl;
			typedef std::tuple<std::string, std::string> hash_entry;

			// std::cout << "i2" << std::endl;
			std::priority_queue<hash_entry, std::vector<hash_entry>, less_than_hash_entry> pri_queue;

			// std::cout << "i3" << std::endl;
			// FULL SCAN
			for (std::uint32_t i = 0; i < directory_size; i++) {			
				// std::cout << "i31" << std::endl;
				#ifdef _USE_LOCAL_LOCKS
				boost::shared_lock<boost::shared_mutex> local_shared_lock(bucket_mutexes[i]);
				#endif
				// std::cout << "i32" << std::endl;
				// std::cout << directory.size() << std::endl;
				// std::cout << i << std::endl;
				if (directory[i]) {
					for (std::uint32_t j = 0; j < directory[i]->keys.size(); j++) {
						// std::cout << "i33" << std::endl;
						if (directory[i]->keys[j] >= start_key && (!end_key || directory[i]->keys[j] <= *end_key)) {
							// std::cout << "i34" << std::endl;
							pri_queue.push(std::make_tuple(directory[i]->keys[j], directory[i]->values[j]));
							// std::cout << "i35" << std::endl;
						}
						// std::cout << "i36" << std::endl;
					}
				}
				#ifdef _USE_LOCAL_LOCKS
				local_shared_lock.unlock();
				#endif
			}
			// std::cout << "i4" << std::endl;
			// Apply push op
			while(!pri_queue.empty()) {
				hash_entry current = pri_queue.top();
				std::string key   = std::get<0>(current);
				std::string value = std::get<1>(current);
				const char* keyp = key.c_str();
				if (!apo.invoke(keyp, key.size(), value)) {
					return;
				}
				pri_queue.pop();
			}
			// std::cout << "i5" << std::endl;
		}

		void reverse_range_scan(const std::string& start_key, const std::string* end_key, abstract_push_op& apo) override{
			if (end_key) assert(*end_key > start_key);
			// std::cout << "i1" << std::endl;
			typedef std::tuple<std::string, std::string> hash_entry;

			// std::cout << "i2" << std::endl;
			std::priority_queue<hash_entry, std::vector<hash_entry>, greater_than_hash_entry> pri_queue;

			// std::cout << "i3" << std::endl;
			// FULL SCAN
			for (std::uint32_t i = 0; i < directory_size; i++) {			
				// std::cout << "i31" << std::endl;
				#ifdef _USE_LOCAL_LOCKS
				boost::shared_lock<boost::shared_mutex> local_shared_lock(bucket_mutexes[i]);
				#endif
				// std::cout << "i32" << std::endl;
				// std::cout << directory.size() << std::endl;
				// std::cout << i << std::endl;
				if (directory[i]) {
					for (std::uint32_t j = 0; j < directory[i]->keys.size(); j++) {
						// std::cout << "i33" << std::endl;
						if (directory[i]->keys[j] >= start_key && (!end_key || directory[i]->keys[j] <= *end_key)) {
							// std::cout << "i34" << std::endl;
							pri_queue.push(std::make_tuple(directory[i]->keys[j], directory[i]->values[j]));
							// std::cout << "i35" << std::endl;
						}
						// std::cout << "i36" << std::endl;
					}
				}
				#ifdef _USE_LOCAL_LOCKS
				local_shared_lock.unlock();
				#endif
			}
			// std::cout << "i4" << std::endl;
			// Apply push op
			while(!pri_queue.empty()) {
				hash_entry current = pri_queue.top();
				std::string key   = std::get<0>(current);
				std::string value = std::get<1>(current);
				const char* keyp = key.c_str();
				if (!apo.invoke(keyp, key.size(), value)) {
					return;
				}
				pri_queue.pop();
			}
			// std::cout << "i5" << std::endl;
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
