/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2016 Vivek Shah (University of Copenhagen)
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:

 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.

 * THE SOFTWARE dbindex IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 * 
 * bplustreeindex.h
 *
 *  Created on: May 30, 2016
 *  Author: Vivek Shah <bonii @ di.ku.dk>
 */
#ifndef SRC_ORDERED_INDEX_BPLUSTREEINDEX_H_
#define SRC_ORDERED_INDEX_BPLUSTREEINDEX_H_

#include "../abstract_index.h"
#include <cstring>

namespace dbindex {
    namespace bplustree {

        struct node;
        //Forward declare to break cyclic dependency

        constexpr std::size_t keys_per_node = 10;

        struct node_val {
            std::string key;
            std::string value;
            node* left_child_node { nullptr };
        };

        struct node {
            node_val values[keys_per_node];
            std::size_t num_entries { 0 };
            node* right_child_node { nullptr };
            node* prev { nullptr };
            node* next { nullptr };
            bool is_leaf_node { false };
            bool is_full { false };
        };

        class bplustreeindex: public abstract_index {
            node* root_node { nullptr };

            node* find_leaf_node_with_key(const std::string& key, node* a_node);

            void scan_till_key(node* leaf_node, size_t index_in_leaf_node,
                    abstract_push_op&, const std::string* end_key, bool reverse_scan = false);

            inline int bytecomparer(const std::string& key1,
                    const std::string& key2) {
                auto min_key_length =
                        key1.size() < key2.size() ? key1.size() : key2.size();
                return std::memcmp(key1.data(), key2.data(), min_key_length);
            }

            inline int bytecomparer(const std::string& key1,
                    const std::string* key2) {
                if (key2 == nullptr) {
                    //Nullptr represents last key present
                    return -1;
                }
                auto min_key_length =
                        key1.size() < key2->size() ? key1.size() : key2->size();
                return std::memcmp(key1.data(), key2->data(), min_key_length);
            }

        public:

            bool get(const std::string& key, std::string& value) override;

            void update(const std::string& key, const std::string& value)
                    override;

            void insert(const std::string& key, const std::string& value)
                    override;

            void remove(const std::string& key) override;

            void range_scan(const std::string& start_key,
                    const std::string* end_key, abstract_push_op&) override;

            void reverse_range_scan(const std::string& start_key,
                    const std::string* end_key, abstract_push_op&) override;

        };
    }
}

#endif /* SRC_ORDERED_INDEX_BPLUSTREEINDEX_H_ */
