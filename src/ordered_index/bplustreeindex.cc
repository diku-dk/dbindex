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
 * bplustree.cc
 *
 *  Created on: May 30, 2016
 *  Author: Vivek Shah <bonii @ di.ku.dk>
 */

#include "bplustreeindex.h"

#include "../util/exception.h"

bool dbindex::bplustree::bplustreeindex::get(const std::string& key,
        std::string& value) {
    auto cur_node = root_node;
    while (cur_node != nullptr) {
        for (auto i = 0; i < cur_node->num_entries; i++) {
            auto& a_node_element = cur_node->values[i];
            auto comparison_result = bytecomparer(key, a_node_element.key);
            if (comparison_result < 0) {
                if (cur_node->is_leaf_node) {
                    return false;
                } else {
                    cur_node = a_node_element.left_child_node;
                }
            } else if (comparison_result == 0) {
                if (cur_node->is_leaf_node) {
                    //Found copy and return
                    value.resize(a_node_element.value.size());
                    std::memcpy(value.data, a_node_element.value.data(),
                            a_node_element.value.size());
                    return true;
                }
            }
            if (i == cur_node->num_entries) {
                cur_node = cur_node->right_child_node;
            }
        }
    }

    return false;
}

void dbindex::bplustree::bplustreeindex::update(const std::string& key,
        const std::string& value) {
    auto cur_node = root_node;
    while (cur_node != nullptr) {
        for (auto i = 0; i < cur_node->num_entries; i++) {
            auto& a_node_element = cur_node->values[i];
            auto comparison_result = bytecomparer(key, a_node_element.key);
            if (comparison_result < 0) {
                if (cur_node->is_leaf_node) {
                    throw std::invalid_argument("Key not found");
                } else {
                    cur_node = a_node_element.left_child_node;
                }
            } else if (comparison_result == 0) {
                if (cur_node->is_leaf_node) {
                    //Found copy and return
                    a_node_element.value.resize(value.size());
                    std::memcpy(a_node_element.value.data, value.data(),
                            value.size());
                    return;
                }
            }
            if (i == cur_node->num_entries) {
                cur_node = cur_node->right_child_node;
            }
        }
    }
    throw std::invalid_argument("Key not found");
}

void dbindex::bplustree::bplustreeindex::insert(const std::string& key,
        const std::string& value) {
    auto a_node = find(key, root_node);
    throw util::not_implemented_exception();
}

void dbindex::bplustree::bplustreeindex::remove(const std::string& key) {
    throw util::not_implemented_exception();
}

void dbindex::bplustree::bplustreeindex::range_scan(
        const std::string& start_key, const std::string* end_key,
        abstract_push_op& op) {
    throw util::not_implemented_exception();
}

void dbindex::bplustree::bplustreeindex::reverse_range_scan(
        const std::string& start_key, const std::string* end_key,
        abstract_push_op& op) {
    throw util::not_implemented_exception();
}

