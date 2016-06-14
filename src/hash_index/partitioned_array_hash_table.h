#ifndef partitioned_array_hash_table_h
#define partitioned_array_hash_table_h

#include <iostream>
#include <functional>
#include <queue>
#include <vector>
#include <boost/thread.hpp>

#include "../macros.h"

#include "../abstract_index.h"
#include "../push_ops.h"

typedef std::uint32_t hash_value_t;

namespace dbindex {
	template<std::uint8_t prefix_bits, std::uint32_t directory_size>
	class partitioned_array_hash_table : public abstract_index {
	private:
		static constexpr std::uint32_t hash_table_amount = 1<<prefix_bits;

		std::vector< array_hash_table<directory_size> > hash_tables;
	public:
		partitioned_array_hash_table(abstract_hash<hash_value_t>& _hash){
			static_assert(prefix_bits <= sizeof(std::uint32_t)*8, "Cannot have that many prefix_bits!");
			hash_tables.reserve(hash_table_amount);
			for (std::uint32_t i = 0; i < hash_table_amount; i++) {
				hash_tables.emplace_back(_hash);
			}
		}
			
		~partitioned_array_hash_table() {
		}

		std::uint32_t create_bit_mask(std::uint32_t b)
        {
            std::uint32_t r = 0;
            for (std::uint32_t i=0; i<=b; i++)
                r |= 1 << i;
            return r;
        }

		void split_prefix_key(const std::string& key, std::uint32_t& prefix_result, std::string& suffix_key) {
			if (prefix_bits >= key.size()*8)
				std::cout << "Key: " << key << ", " << key.size() <<  std::endl;
			assert(prefix_bits < key.size()*8);
			auto ukey = reinterpret_cast<const uint8_t*>(key.c_str());
            prefix_result = 0;

            // Head of prefix:
            const std::uint8_t head_blocks = prefix_bits/8;
           	const std::uint8_t tail_bits = prefix_bits%8;
            
           	// Ensure no overflow of the prefix_result
            assert(head_blocks < sizeof(std::uint32_t) || (head_blocks == sizeof(std::uint32_t) && tail_bits == 0));

            // std::cout << "head_blocks: " << (int)head_blocks << std::endl;
            const std::uint8_t* blocks = (const std::uint8_t*) (ukey + head_blocks);

            for (std::int8_t i = -head_blocks; i < -1; i++) {
                prefix_result += blocks[i];
            	// std::cout << "Iteration: " << (int)i << ", added: " << (int)blocks[i] << std::endl;
                prefix_result <<= 8;
            	// std::cout << "new prefix: " << prefix_result << std::endl;
            }
            if (head_blocks > 0)
            	prefix_result += blocks[-1];
            // std::cout << "post-head prefix: " << prefix_result << std::endl;

            // Tail of prefix:
           	if (tail_bits != 0) {
                prefix_result <<= 8;
           		// std::cout << "tail_bits: " << (int)tail_bits << std::endl;
            	// std::cout << "tail: " << (int)blocks[0] << std::endl;
           		prefix_result += blocks[0];
           		prefix_result >>= 8-tail_bits;
            	// std::cout << "post-tail prefix: " << prefix_result << std::endl; 
            }
           	// Remainder of string.
           	suffix_key = key.substr(head_blocks);
           	suffix_key[0] = (char)(blocks[0] & create_bit_mask(7-tail_bits));
		}

		std::string generate_key_prefix(std::uint32_t prefix_int) {
			std::string prefix_str = "    ";
			prefix_str[0] = (prefix_int >> 24) & 0xFF;
			prefix_str[1] = (prefix_int >> 16) & 0xFF;
			prefix_str[2] = (prefix_int >> 8) & 0xFF;
			prefix_str[3] = (prefix_int & 0xFF) << (8-(prefix_bits % 8));
			return prefix_str;
		}

		bool get(const std::string& key, std::string& value) override {
			std::uint32_t prefix_result;
			std::string suffix_key;
				
			split_prefix_key(key, prefix_result, suffix_key);
			return hash_tables[prefix_result].get(suffix_key, value);
		}

		void insert(const std::string& key, const std::string& new_value) override {
			std::uint32_t prefix_result;
			std::string suffix_key;
			split_prefix_key(key, prefix_result, suffix_key);
			// std::cout << "Key: " << key << ", first char: " << key[0] << ", int of first char: " << (int)key[0] << ", prefix: " << prefix_result << ", suffix_key: " << suffix_key << std::endl;
			hash_tables[prefix_result].insert(suffix_key, new_value);
		}

		void update(const std::string& key, const std::string& new_value) override {
			std::uint32_t prefix_result;
			std::string suffix_key;
			split_prefix_key(key, prefix_result, suffix_key);
			hash_tables[prefix_result].update(suffix_key, new_value);
		}

		void remove(const std::string& key) override {
			std::uint32_t prefix_result;
			std::string suffix_key;
			split_prefix_key(key, prefix_result, suffix_key);
			hash_tables[prefix_result].remove(suffix_key);
		}

		void range_scan(const std::string& start_key, const std::string* end_key, abstract_push_op& apo) override{
			if (end_key) assert(*end_key > start_key);
			
			// std::cout << "1" << std::endl;

			std::uint32_t start_prefix_result;
			std::string start_suffix_key;
			// std::cout << "2" << std::endl;
			if (start_key == "") {
				start_prefix_result = 0;
				start_suffix_key = "";
			} else {
				split_prefix_key(start_key, start_prefix_result, start_suffix_key);
			}
			// std::cout << "3" << std::endl;
			partitioned_push_op<less_than_hash_entry> po{generate_key_prefix(start_prefix_result)};
			// std::cout << "start_prefix_result: " << start_prefix_result << std::endl;
			// std::cout << "startkey: " << start_key << std::endl;
			// std::cout << "start_prefix_result: " << start_prefix_result << std::endl;
			// std::cout << "start_suffix_key: " << start_suffix_key << std::endl;
			// std::cout << "4" << std::endl;
			if (end_key) { // Closed-end scan
				// std::cout << "411" << std::endl;
				std::uint32_t end_prefix_result;
				std::string end_suffix_key;
				split_prefix_key(*end_key, end_prefix_result, end_suffix_key);
				// std::cout << "412" << std::endl;

				const std::string* end_suffix_keyp = &end_suffix_key;

				if (start_prefix_result == end_prefix_result) { // Only one partition
					// std::cout << "Only bucket: " << start_prefix_result << std::endl;
					// std::cout << "4131" << std::endl;
					hash_tables[start_prefix_result].range_scan(start_suffix_key, end_suffix_keyp, po);
					// std::cout << "4132" << std::endl;
				} else { // Multiple partitions
					// std::cout << "4141" << std::endl;
					// std::cout << "spr: " << start_prefix_result << ", hta: " << hash_table_amount << std::endl;
					hash_tables[start_prefix_result].range_scan(start_suffix_key, NULL, po);
					// std::cout << "First bucket: " << start_prefix_result << ", then buckets: ";
					// std::cout << "4142" << std::endl;
					for (std::uint32_t i = start_prefix_result+1; i < end_prefix_result; i++) {
						// std::cout << i << ", ";
						po.set_prefix(generate_key_prefix(i));
						hash_tables[i].range_scan("", NULL, po);
					}
					// std::cout << "\nFinally bucket: " << end_prefix_result << std::endl;
					// std::cout << "4143" << std::endl;
					po.set_prefix(generate_key_prefix(end_prefix_result));
					hash_tables[end_prefix_result].range_scan("", end_suffix_keyp, po);
					// std::cout << "4144" << std::endl;
				}
			} else { // Open-ended scan
				// std::cout << "421" << std::endl;
				// std::cout << "First bucket: " << start_prefix_result << ", then buckets: ";
				hash_tables[start_prefix_result].range_scan(start_suffix_key, NULL, po);
				// std::cout << "422" << std::endl;
				for (std::uint32_t i = start_prefix_result+1; i < hash_table_amount; i++) {
					// std::cout << i << ", ";
					po.set_prefix(generate_key_prefix(i));
					hash_tables[i].range_scan("", NULL, po);
				}
				// std::cout << "424" << std::endl;
			}

			// std::cout << "5" << std::endl;
			// push results
			auto queue = po.get();
			// std::cout << "6" << std::endl;

			while (!queue.empty())
			{
				hash_entry entry = queue.top();
				std::string key   = std::get<0>(entry);
				std::string value = std::get<1>(entry);
				// std::cout << "Value: " << value << std::endl;
				// std::cout << "8" << std::endl;
				const char* keyp = key.c_str();
				if (!apo.invoke(keyp, key.size(), value)) {
					return;
				}
				// std::cout << "9" << std::endl;
				queue.pop();
			}
			// std::cout << "10" << std::endl;
		}

		void reverse_range_scan(const std::string& start_key, const std::string* end_key, abstract_push_op& apo) override{
			if (end_key) assert(*end_key > start_key);

			// std::cout << "1" << std::endl;

			std::uint32_t start_prefix_result;
			std::string start_suffix_key;
			// std::cout << "2" << std::endl;
			split_prefix_key(start_key, start_prefix_result, start_suffix_key);
			// std::cout << "3" << std::endl;
			partitioned_push_op<greater_than_hash_entry> po{generate_key_prefix(start_prefix_result)};
			// std::cout << "start_prefix_result: " << start_prefix_result << std::endl;
			// std::cout << "startkey: " << start_key << std::endl;
			// std::cout << "start_prefix_result: " << start_prefix_result << std::endl;
			// std::cout << "start_suffix_key: " << start_suffix_key << std::endl;
			// std::cout << "4" << std::endl;
			if (end_key) { // Closed-end scan
				// std::cout << "411" << std::endl;
				std::uint32_t end_prefix_result;
				std::string end_suffix_key;
				split_prefix_key(*end_key, end_prefix_result, end_suffix_key);
				// std::cout << "412" << std::endl;

				const std::string* end_suffix_keyp = &end_suffix_key;

				if (start_prefix_result == end_prefix_result) { // Only one partition
					// std::cout << "Only bucket: " << start_prefix_result << std::endl;
					// std::cout << "4131" << std::endl;
					hash_tables[start_prefix_result].reverse_range_scan(start_suffix_key, end_suffix_keyp, po);
					// std::cout << "4132" << std::endl;
				} else { // Multiple partitions
					// std::cout << "4141" << std::endl;
					// std::cout << "spr: " << start_prefix_result << ", hta: " << hash_table_amount << std::endl;
					hash_tables[start_prefix_result].reverse_range_scan(start_suffix_key, NULL, po);
					// std::cout << "First bucket: " << start_prefix_result << ", then buckets: ";
					// std::cout << "4142" << std::endl;
					for (std::uint32_t i = start_prefix_result+1; i < end_prefix_result; i++) {
						// std::cout << i << ", ";
						po.set_prefix(generate_key_prefix(i));
						hash_tables[i].reverse_range_scan("", NULL, po);
					}
					// std::cout << "\nFinally bucket: " << end_prefix_result << std::endl;
					// std::cout << "4143" << std::endl;
					po.set_prefix(generate_key_prefix(end_prefix_result));
					hash_tables[end_prefix_result].reverse_range_scan("", end_suffix_keyp, po);
					// std::cout << "4144" << std::endl;
				}
			} else { // Open-ended scan
				// std::cout << "421" << std::endl;
				// std::cout << "First bucket: " << start_prefix_result << ", then buckets: ";
				hash_tables[start_prefix_result].reverse_range_scan(start_suffix_key, NULL, po);
				// std::cout << "422" << std::endl;
				for (std::uint32_t i = start_prefix_result+1; i < hash_table_amount; i++) {
					// std::cout << i << ", ";
					po.set_prefix(generate_key_prefix(i));
					hash_tables[i].reverse_range_scan("", NULL, po);
				}
				// std::cout << "424" << std::endl;
			}

			// std::cout << "5" << std::endl;
			// push results
			auto queue = po.get();
			// std::cout << "6" << std::endl;

			while (!queue.empty())
			{
				hash_entry entry = queue.top();
				std::string key   = std::get<0>(entry);
				std::string value = std::get<1>(entry);
				// std::cout << "Value: " << key << std::endl;
				// std::cout << "8" << std::endl;
				const char* keyp = key.c_str();
				if (!apo.invoke(keyp, key.size(), value)) {
					return;
				}
				// std::cout << "9" << std::endl;
				queue.pop();
			}
			// std::cout << "10" << std::endl;
		}

		std::uint32_t get_directory_size() {
			return directory_size;
		}

		size_t size() {
			std::uint32_t total_size = 0;
			for (typename std::vector<array_hash_table<directory_size>>::iterator it = hash_tables.begin(); it != hash_tables.end(); ++it) {
				total_size += (*it).size();
			}
			return total_size;
		}

        std::string to_string() override {
            return "partitioned_array_hash_table";
        }

	};
}

#endif
