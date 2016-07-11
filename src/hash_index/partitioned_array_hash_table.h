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

		std::uint32_t calc_prefix_of_key(const std::string& key) {
			if (prefix_bits >= key.size()*8)
			assert(prefix_bits < key.size()*8);
			auto ukey = reinterpret_cast<const uint8_t*>(key.c_str());
            std::uint32_t prefix_result = 0;

            // Head of prefix:
            const std::uint8_t head_blocks = prefix_bits/8;
           	const std::uint8_t tail_bits = prefix_bits%8;
            
           	// Ensure no overflow of the prefix_result
            assert(head_blocks < sizeof(std::uint32_t) || (head_blocks == sizeof(std::uint32_t) && tail_bits == 0));

            const std::uint8_t* blocks = (const std::uint8_t*) (ukey + head_blocks);

            for (std::int8_t i = -head_blocks; i < -1; i++) {
                prefix_result += blocks[i];
                prefix_result <<= 8;
            }
            if (head_blocks > 0)
            	prefix_result += blocks[-1];

            // Tail of prefix:
           	if (tail_bits != 0) {
                prefix_result <<= 8;
           		prefix_result += blocks[0];
           		prefix_result >>= 8-tail_bits;
            }
            return prefix_result;
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
			std::uint32_t prefix_result = calc_prefix_of_key(key);
			return hash_tables[prefix_result].get(key, value);
		}

		void insert(const std::string& key, const std::string& new_value) override {
			std::uint32_t prefix_result = calc_prefix_of_key(key);
			// std::cout << "Key: " << key << ", first char: " << key[0] << ", int of first char: " << (int)key[0] << ", prefix: " << prefix_result << ", suffix_key: " << suffix_key << std::endl;
			hash_tables[prefix_result].insert(key, new_value);
		}

		void update(const std::string& key, const std::string& new_value) override {
			std::uint32_t prefix_result = calc_prefix_of_key(key);
			
			hash_tables[prefix_result].update(key, new_value);
		}

		void remove(const std::string& key) override {
			std::uint32_t prefix_result = calc_prefix_of_key(key);
			hash_tables[prefix_result].remove(key);
		}

		void range_scan(const std::string& start_key, const std::string* end_key, abstract_push_op& apo) override{
			if (end_key) assert(*end_key > start_key);
			

			std::uint32_t start_prefix_result;
			if (start_key == "") {
				start_prefix_result = 0;
			} else {
				start_prefix_result = calc_prefix_of_key(start_key);
			}
			partitioned_push_op<less_than_hash_entry> po{};
			if (end_key) { // Closed-end scan
				std::uint32_t end_prefix_result = calc_prefix_of_key(*end_key);

				if (start_prefix_result == end_prefix_result) { // Only one partition
					hash_tables[start_prefix_result].range_scan(start_key, end_key, po);
					if (!push_queue_op<less_than_hash_entry>(po.get(), apo)){
						return;
					}
				} else { // Multiple partitions
					hash_tables[start_prefix_result].range_scan(start_key, nullptr, po);
					if (!push_queue_op<less_than_hash_entry>(po.get(), apo)){
						return;
					}

					for (std::uint32_t i = start_prefix_result+1; i < end_prefix_result; i++) {
						hash_tables[i].range_scan("", nullptr, po);
						if (!push_queue_op<less_than_hash_entry>(po.get(), apo)){
							return;
						}
					}
					hash_tables[end_prefix_result].range_scan("", end_key, po);
					if (!push_queue_op<less_than_hash_entry>(po.get(), apo)){
						return;
					}
				}
			} else { // Open-ended scan
				hash_tables[start_prefix_result].range_scan(start_key, nullptr, po);
				if (!push_queue_op<less_than_hash_entry>(po.get(), apo)){
					return;
				}
				for (std::uint32_t i = start_prefix_result+1; i < hash_table_amount; i++) {
					hash_tables[i].range_scan("", nullptr, po);
					if (!push_queue_op<less_than_hash_entry>(po.get(), apo)){
						return;
					}
				}
			}

		}
		void reverse_range_scan(const std::string& start_key, const std::string* end_key, abstract_push_op& apo) override{
			if (end_key) assert(*end_key > start_key);
			

			std::uint32_t start_prefix_result;
			if (start_key == "") {
				start_prefix_result = 0;
			} else {
				start_prefix_result = calc_prefix_of_key(start_key);
			}
			partitioned_push_op<greater_than_hash_entry> po{};
			if (end_key) { // Closed-end scan
				std::uint32_t end_prefix_result = calc_prefix_of_key(*end_key);

				if (start_prefix_result == end_prefix_result) { // Only one partition
					hash_tables[start_prefix_result].reverse_range_scan(start_key, end_key, po);
					if (!push_queue_op<greater_than_hash_entry>(po.get(), apo))
						return;
				} else { // Multiple partitions

					hash_tables[end_prefix_result].reverse_range_scan("", end_key, po);
					if (!push_queue_op<greater_than_hash_entry>(po.get(), apo))
						return;

					for (std::uint32_t i = end_prefix_result-1; i > start_prefix_result; i--) {
						hash_tables[i].reverse_range_scan("", nullptr, po);
						if (!push_queue_op<greater_than_hash_entry>(po.get(), apo))
							return;
					}
					hash_tables[start_prefix_result].reverse_range_scan(start_key, nullptr, po);
					if (!push_queue_op<greater_than_hash_entry>(po.get(), apo))
						return;
				}
			} else { // Open-ended scan
				for (std::uint32_t i = hash_table_amount-1; i > start_prefix_result; i--) {
					hash_tables[i].reverse_range_scan("", nullptr, po);
					if (!push_queue_op<greater_than_hash_entry>(po.get(), apo))
						return;
				}
				
				hash_tables[start_prefix_result].reverse_range_scan(start_key, nullptr, po);
				if (!push_queue_op<greater_than_hash_entry>(po.get(), apo))
					return;
			}
		}

		template <typename cmp>
		bool push_queue_op(std::priority_queue<hash_entry, std::vector<hash_entry>, cmp> queue, abstract_push_op& apo) {
			while (!queue.empty())
			{
				hash_entry entry = queue.top();
				std::string key   = std::get<0>(entry);
				std::string value = std::get<1>(entry);
				const char* keyp = key.c_str();
				if (!apo.invoke(keyp, key.size(), value)) {
					return false;
				}
				queue.pop();
			}
			return true;
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
	};
}

#endif
