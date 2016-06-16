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

/**
 * Returns the leaf node that can contain the value
 */
dbindex::bplustree::node* dbindex::bplustree::bplustreeindex::find_leaf_node_with_key(
        const std::string& key, node* start_node) {
    if (start_node == nullptr || start_node->is_leaf_node) {
        return start_node;
    }

    for (std::size_t i = 0; i < start_node->num_entries; i++) {
        auto comparison_result = bytecomparer(key, start_node->values[i].key);
        if (comparison_result < 0) {
            //Look at the left subtree
            return find_leaf_node_with_key(key,
                    start_node->values[i].left_child_node);
        }
    }
    //Did not find it in the left child entries per node_val must look at the rightmost entry
    return find_leaf_node_with_key(key, start_node->right_child_node);
}

bool dbindex::bplustree::bplustreeindex::get(const std::string& key,
        std::string& value) {
    auto leaf_node_with_key = find_leaf_node_with_key(key, root_node);
    if (leaf_node_with_key == nullptr) {
        return false;
    }

    for (std::size_t i = 0; i < leaf_node_with_key->num_entries; i++) {
        auto& target_element = leaf_node_with_key->values[i];
        auto comparison_result = bytecomparer(key, target_element.key);
        if (comparison_result == 0) {
            //Found copy and return
            value.assign(target_element.value);
            return true;
        }
    }
    return false;
}

void dbindex::bplustree::bplustreeindex::update(const std::string& key,
        const std::string& value) {
    auto leaf_node_with_key = find_leaf_node_with_key(key, root_node);
    if (leaf_node_with_key == nullptr) {
        throw std::invalid_argument("Key not found");
    }

    for (std::size_t i = 0; i < leaf_node_with_key->num_entries; i++) {
        auto& target_element = leaf_node_with_key->values[i];
        auto comparison_result = bytecomparer(key, target_element.key);
        if (comparison_result == 0) {
            target_element.value.assign(value);
            return;
        }
    }
    throw std::invalid_argument("Key not found");
}

void dbindex::bplustree::bplustreeindex::insert(const std::string& key,
        const std::string& value) {
    throw util::not_implemented_exception();
}

void dbindex::bplustree::bplustreeindex::remove(const std::string& key) {
    std::string empty_value { "" };
    update(key, empty_value);
}

void dbindex::bplustree::bplustreeindex::scan_till_key(node* cur_leaf_node,
        size_t index_in_leaf_node, abstract_push_op& op,
        const std::string* end_key, bool reverse_scan) {
    size_t step_by = reverse_scan ? -1 : 1;

    while (cur_leaf_node != nullptr) {
        //Simulate both forward and backward loop with step_by
        for (std::size_t i = index_in_leaf_node;
                i < cur_leaf_node->num_entries && i >= 0; i += step_by) {
            auto& target_element = cur_leaf_node->values[i];
            auto comparison_result = bytecomparer(target_element.value,
                    end_key);
            if (reverse_scan) {
                if (comparison_result < 0) {
                    return;
                }
            } else {
                if (comparison_result > 0) {
                    return;
                }
            }
            if (!op.invoke(target_element.key.data(), target_element.key.size(),
                    target_element.value)) {
                //Terminate scan
                return;
            }
        }
        //Get to the next leaf node
        if (reverse_scan) {
            cur_leaf_node = cur_leaf_node->prev;
            index_in_leaf_node = cur_leaf_node->num_entries - 1;
        } else {
            cur_leaf_node = cur_leaf_node->next;
            index_in_leaf_node = 0;
        }
    }
}

void dbindex::bplustree::bplustreeindex::range_scan(
        const std::string& start_key, const std::string* end_key,
        abstract_push_op& op) {
    auto leaf_node_with_key = find_leaf_node_with_key(start_key, root_node);

    if (leaf_node_with_key == nullptr) {
        return;
    }

    while (leaf_node_with_key != nullptr) {
        for (std::size_t i = 0; i < leaf_node_with_key->num_entries; i++) {
            auto& cur_element = leaf_node_with_key->values[i];
            auto comparison_result = bytecomparer(start_key, cur_element.key);
            if (comparison_result >= 0) {
                scan_till_key(leaf_node_with_key, i, op, end_key);
                return;
            }
        }
    }
}

void dbindex::bplustree::bplustreeindex::reverse_range_scan(
        const std::string& start_key, const std::string* end_key,
        abstract_push_op& op) {
    auto leaf_node_with_key = find_leaf_node_with_key(start_key, root_node);

    if (leaf_node_with_key == nullptr) {
        return;
    }

    while (leaf_node_with_key != nullptr) {
        for (std::size_t i = leaf_node_with_key->num_entries - 1; i >= 0; i--) {
            auto& cur_element = leaf_node_with_key->values[i];
            auto comparison_result = bytecomparer(cur_element.key, start_key);
            if (comparison_result <= 0) {
                scan_till_key(leaf_node_with_key, i, op, end_key);
                return;
            }
        }
    }

}

