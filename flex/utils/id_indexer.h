/** Copyright 2020 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#ifndef GRAPHSCOPE_GRAPH_ID_INDEXER_H_
#define GRAPHSCOPE_GRAPH_ID_INDEXER_H_

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cmath>
#include <memory>
#include <mutex>
#include <string_view>
#include <type_traits>
#include <vector>

#include "flat_hash_map/flat_hash_map.hpp"
#include "flex/utils/base_indexer.h"
#include "flex/utils/message_id_indexer.h"
#include "flex/utils/mmap_array.h"
#include "flex/utils/string_view_vector.h"

namespace gs {

template <typename INDEX_T>
class LFIndexer : public BaseIndexer<INDEX_T> {
 public:
  LFIndexer() : num_elements_(0), hasher_() {}
  LFIndexer(LFIndexer&& rhs)
      : keys_(std::move(rhs.keys_)),
        indices_(std::move(rhs.indices_)),
        num_elements_(rhs.num_elements_.load()),
        num_slots_minus_one_(rhs.num_slots_minus_one_),
        hasher_(rhs.hasher_) {
    hash_policy_.set_mod_function_by_index(
        rhs.hash_policy_.get_mod_function_index());
  }
  ~LFIndexer() override { LOG(INFO) << "LFIndexer: destructor called"; }
  size_t size() const override { return num_elements_.load(); }

  INDEX_T insert(int64_t oid) override {
    INDEX_T ind = static_cast<INDEX_T>(num_elements_.fetch_add(1));
#if OV
    keys_.set(ind, oid);
    size_t index =
        hash_policy_.index_for_hash(hasher_(oid), num_slots_minus_one_);
    static constexpr INDEX_T sentinel = std::numeric_limits<INDEX_T>::max();

    while (true) {
      if (__sync_bool_compare_and_swap(&indices_.data()[index], sentinel,
                                       ind)) {
        break;
      }
      index = (index + 1) % num_slots_minus_one_;
    }
#else
    {
      auto item1 = keys_.get(ind);
      gbp::BufferBlock::UpdateContent<int64_t>(
          [&](int64_t& item) { item = oid; }, item1);
    }
    size_t index =
        hash_policy_.index_for_hash(hasher_(oid), num_slots_minus_one_);
    static constexpr INDEX_T sentinel = std::numeric_limits<INDEX_T>::max();

    int mark = 0;
    // TODO: 此处实现未被测试正确性
    uint32_t num_get =
        indices_.OBJ_NUM_PERPAGE - index % indices_.OBJ_NUM_PERPAGE;
    uint32_t start_index = index, end_index = index + num_get;
    auto items = indices_.get(index, num_get);
    while (true) {
      if (unlikely(index < start_index || index >= end_index)) {
        num_get = indices_.OBJ_NUM_PERPAGE - index % indices_.OBJ_NUM_PERPAGE;
        items = indices_.get(index, num_get);
        start_index = index, end_index = index + num_get;
      }

      gbp::BufferBlock::UpdateContent<index_key_item<INDEX_T>>(
          [&](index_key_item<INDEX_T>& item) {
            mark = __sync_bool_compare_and_swap(&(item.index), sentinel, ind);
            if (mark) {
              item.key = oid;
            }
          },
          items, index - start_index);
      if (mark) {
        break;
      }
      index = (index + 1) % num_slots_minus_one_;
    }
#endif
    return ind;
  }
  INDEX_T insert_with_parent_oid(int64_t oid, int64_t foreign_oid) override {
    LOG(FATAL) << "LFIndexer: insert_with_parent_oid is not implemented";
    return 0;
  }
  op_status get_kid_index(size_t kid_label_id, int64_t parent_oid,
                          INDEX_T& ret) override {
    LOG(FATAL) << "LFIndexer: get_kid_index is not implemented";
    return op_status::op_status_not_implemented;
  }
  op_status set_new_kid_range(size_t kid_label_id,
                              int64_t max_kid_oid) override {
    LOG(FATAL) << "LFIndexer: set_new_kid_range is not implemented";
    return op_status::op_status_not_implemented;
  }

  INDEX_T get_index(int64_t oid) const override {
    size_t index =
        hash_policy_.index_for_hash(hasher_(oid), num_slots_minus_one_);
    static constexpr INDEX_T sentinel = std::numeric_limits<INDEX_T>::max();
#if OV
    while (true) {
      INDEX_T ind = indices_.get(index);
      if (ind == sentinel) {
        LOG(FATAL) << "cannot find " << oid << " in id_indexer";
      } else if (keys_.get(ind) == oid) {
        return ind;
      } else {
        index = (index + 1) % num_slots_minus_one_;
      }
    }
#else
    auto num_get = indices_.OBJ_NUM_PERPAGE - index % indices_.OBJ_NUM_PERPAGE;
    num_get = std::min(num_get, indices_.size() - index);
    uint32_t start_index = index, end_index = index + num_get;
    auto items = indices_.get(index, num_get);
    while (true) {
      if (unlikely(index < start_index || index >= end_index)) {
        num_get = indices_.OBJ_NUM_PERPAGE - index % indices_.OBJ_NUM_PERPAGE;
        num_get = std::min(num_get, indices_.size() - index);
        items = indices_.get(index, num_get);
        start_index = index, end_index = index + num_get;
      }

      auto& ind = gbp::BufferBlock::Ref<index_key_item<INDEX_T>>(
          items, index - start_index);
      if (ind.index == sentinel) {
        LOG(FATAL) << "cannot find " << oid << " in id_indexer";
      } else {
        // auto item = keys_.get(ind.index);
        // assert(gbp::BufferBlock::Ref<int64_t>(item) == ind.key);
        if (ind.key == oid) {
          return ind.index;
        }
      }
      index = (index + 1) % num_slots_minus_one_;
    }
#endif
  }

#if OV
  bool get_index(int64_t oid, INDEX_T& ret) const {
    size_t index =
        hash_policy_.index_for_hash(hasher_(oid), num_slots_minus_one_);

    static constexpr INDEX_T sentinel = std::numeric_limits<INDEX_T>::max();
    while (true) {
      INDEX_T ind = indices_.get(index);

      if (ind == sentinel) {
        return false;
      } else if (keys_.get(ind) == oid) {
        ret = ind;
        return true;
      } else {
        index = (index + 1) % num_slots_minus_one_;
      }
    }

    return false;
  }
  int64_t get_key(const INDEX_T& index) const { return keys_.get(index); }
#else
  bool get_index(int64_t oid, INDEX_T& ret) const override {
    size_t index =
        hash_policy_.index_for_hash(hasher_(oid), num_slots_minus_one_);
    static constexpr INDEX_T sentinel = std::numeric_limits<INDEX_T>::max();

    uint32_t num_get =
        indices_.OBJ_NUM_PERPAGE - index % indices_.OBJ_NUM_PERPAGE;
    num_get =
        num_get > indices_.size() - index ? indices_.size() - index : num_get;
    uint32_t start_index = index, end_index = index + num_get;
    auto items = indices_.get(index, num_get);
    while (true) {
      if (unlikely(index < start_index || index >= end_index)) {
        num_get = indices_.OBJ_NUM_PERPAGE - index % indices_.OBJ_NUM_PERPAGE;
        num_get = num_get > indices_.size() - index ? indices_.size() - index
                                                    : num_get;
        items = indices_.get(index, num_get);
        start_index = index, end_index = index + num_get;
      }

      auto& ind = gbp::BufferBlock::Ref<index_key_item<INDEX_T>>(
          items, index - start_index);
      if (ind.index == sentinel) {
        return false;
      } else {
        // auto item = keys_.get(ind.index);
        // assert(gbp::BufferBlock::Ref<int64_t>(item) == ind.key);
        if (ind.key == oid) {
          ret = ind.index;
          return true;
        }
      }
      index = (index + 1) % num_slots_minus_one_;
    }

    return false;
  }

  int64_t get_key(const INDEX_T& index) const override {
    auto item = keys_.get(index);
    return gbp::BufferBlock::Ref<int64_t>(item);
  }
#endif

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) {
    keys_.open(snapshot_dir + "/" + name + ".keys", true);
    keys_.touch(work_dir + "/" + name + ".keys");
    indices_.open(snapshot_dir + "/" + name + ".indices", true);
    indices_.touch(work_dir + "/" + name + ".indices");

    indices_size_ = indices_.size();
    for (size_t k = keys_.size() - 1; k >= 0; --k) {
#if OV
      if (keys_.get(k) != std::numeric_limits<int64_t>::max()) {
        num_elements_.store(k + 1);
        break;
      }
#else
      auto item = keys_.get(k);
      if (gbp::BufferBlock::Ref<int64_t>(item) !=
          std::numeric_limits<int64_t>::max()) {
        num_elements_.store(k + 1);
        break;
      }
#endif
    }

    load_meta(snapshot_dir + "/" + name + ".meta");
  }

  size_t get_size_in_byte() {
    return keys_.get_size_in_byte() + indices_.get_size_in_byte();
  }

  void dump(const std::string& name, const std::string& snapshot_dir) {
    keys_.dump(snapshot_dir + "/" + name + ".keys");
    indices_.dump(snapshot_dir + "/" + name + ".indices");
    dump_meta(snapshot_dir + "/" + name + ".meta");
  }

  void dump_meta(const std::string& filename) const {
    grape::InArchive arc;
    arc << num_slots_minus_one_ << hash_policy_.get_mod_function_index();
    FILE* fout = fopen(filename.c_str(), "wb");
    fwrite(arc.GetBuffer(), sizeof(char), arc.GetSize(), fout);
    fflush(fout);
    fclose(fout);
  }

  void load_meta(const std::string& filename) {
    grape::OutArchive arc;
    FILE* fin = fopen(filename.c_str(), "r");

    size_t meta_file_size = std::filesystem::file_size(filename);

    std::vector<char> buf(meta_file_size);
    CHECK_EQ(fread(buf.data(), sizeof(char), meta_file_size, fin),
             meta_file_size);
    arc.SetSlice(buf.data(), meta_file_size);
    size_t mod_function_index;
    arc >> num_slots_minus_one_ >> mod_function_index;
    hash_policy_.set_mod_function_by_index(mod_function_index);
  }

  // get keys
  const mmap_array<int64_t>& get_keys() const { return keys_; }

 private:
  mmap_array<int64_t> keys_;
#if OV
  mmap_array<INDEX_T>
      indices_;  // size() == indices_size_ == num_slots_minus_one_
                 // +log(num_slots_minus_one_)
#else
  mmap_array<index_key_item<INDEX_T>>
      indices_;  // size() == indices_size_ == num_slots_minus_one_
                 // +log(num_slots_minus_one_)
#endif
  std::atomic<size_t> num_elements_;
  size_t num_slots_minus_one_;
  size_t indices_size_;
  ska::ska::prime_number_hash_policy hash_policy_;
  GHash<int64_t> hasher_;

  template <typename _INDEX_T>
  friend void build_lf_indexer(const IdIndexer<int64_t, _INDEX_T>& input,
                               const std::string& filename,
                               LFIndexer<_INDEX_T>& output, double rate);
};

#if OV
template <class INDEX_T>
void build_lf_indexer(const IdIndexer<int64_t, INDEX_T>& input,
                      const std::string& filename, LFIndexer<INDEX_T>& lf,
                      double rate) {
  double indices_rate = static_cast<double>(input.keys_.size()) /
                        static_cast<double>(input.indices_.size());
  CHECK_LT(indices_rate, rate);

  size_t size = input.keys_.size();
  size_t lf_size = static_cast<double>(size) / rate + 1;
  lf_size = std::max(lf_size, static_cast<size_t>(1024));

  lf.keys_.open(filename + ".keys", false);
  lf.keys_.resize(lf_size);
  memcpy(lf.keys_.data(), input.keys_.data(), sizeof(int64_t) * size);
  for (size_t k = size; k != lf_size; ++k) {
    lf.keys_.set(k, std::numeric_limits<int64_t>::max());
  }

  lf.num_elements_.store(size);

  lf.indices_.open(filename + ".indices", false);
  lf.indices_.resize(input.indices_.size());
  for (size_t k = 0; k != input.indices_.size(); ++k) {
    lf.indices_[k] = std::numeric_limits<INDEX_T>::max();
  }
  lf.indices_size_ = input.indices_.size();

  lf.hash_policy_.set_mod_function_by_index(
      input.hash_policy_.get_mod_function_index());
  lf.num_slots_minus_one_ = input.num_slots_minus_one_;
  std::vector<std::pair<int64_t, INDEX_T>> extra;

  for (auto oid : input.keys_) {
    size_t index = input.hash_policy_.index_for_hash(
        input.hasher_(oid), input.num_slots_minus_one_);
    for (int8_t distance = 0; input.distances_[index] >= distance;
         ++distance, ++index) {
      INDEX_T ret = input.indices_[index];
      if (input.keys_[ret] == oid) {
        if (index >= input.num_slots_minus_one_) {
          extra.emplace_back(oid, ret);
        } else {
          lf.indices_[index] = ret;
        }
        break;
      }
    }
  }

  static constexpr INDEX_T sentinel = std::numeric_limits<INDEX_T>::max();
  for (auto& pair : extra) {
    size_t index = input.hash_policy_.index_for_hash(
        input.hasher_(pair.first), input.num_slots_minus_one_);
    while (true) {
      if (lf.indices_[index] == sentinel) {
        lf.indices_[index] = pair.second;
        break;
      }
      index = (index + 1) % input.num_slots_minus_one_;
    }
  }

  lf.dump_meta(filename + ".meta");
}
#else
template <class INDEX_T>
void build_lf_indexer(const IdIndexer<int64_t, INDEX_T>& input,
                      const std::string& filename, LFIndexer<INDEX_T>& lf,
                      double rate) {
  double indices_rate = static_cast<double>(input.keys_.size()) /
                        static_cast<double>(input.indices_.size());
  CHECK_LT(indices_rate, rate);

  size_t size = input.keys_.size();
  size_t lf_size = static_cast<double>(size) / rate + 1;
  lf_size = std::max(lf_size, static_cast<size_t>(1024));

  lf.keys_.open(filename + ".keys", false);
  lf.keys_.resize(lf_size);

  // FIXME: input.keys_是mmap分配的还是malloc分配的？？？
  auto keys_item = lf.keys_.get(0, lf_size);
  for (size_t t = 0; t < size; t++) {
    gbp::BufferBlock::UpdateContent<int64_t>(
        [&](int64_t& item) { item = input.keys_[t]; }, keys_item, t);
  }

  for (size_t k = size; k != lf_size; ++k) {
    gbp::BufferBlock::UpdateContent<int64_t>(
        [&](int64_t& item) { item = std::numeric_limits<int64_t>::max(); },
        keys_item, k);
  }

  lf.num_elements_.store(size);

  lf.indices_.open(filename + ".indices", false);
  lf.indices_.resize(input.indices_.size());

  index_key_item<INDEX_T> empty_value_1{std::numeric_limits<INDEX_T>::max(), 0};
  for (size_t k = 0; k != input.indices_.size(); ++k) {
    lf.indices_.set(k, &empty_value_1);
  }
  lf.indices_size_ = input.indices_.size();

  lf.hash_policy_.set_mod_function_by_index(
      input.hash_policy_.get_mod_function_index());
  lf.num_slots_minus_one_ = input.num_slots_minus_one_;
  std::vector<std::pair<int64_t, INDEX_T>> extra;

  for (auto oid : input.keys_) {
    size_t index = input.hash_policy_.index_for_hash(
        input.hasher_(oid), input.num_slots_minus_one_);
    for (int8_t distance = 0; input.distances_[index] >= distance;
         ++distance, ++index) {
      INDEX_T ret = input.indices_[index];
      if (input.keys_[ret] == oid) {
        if (index >= input.num_slots_minus_one_) {
          extra.emplace_back(oid, ret);
        } else {
          empty_value_1.index = ret;
          empty_value_1.key = oid;
          lf.indices_.set(index, &empty_value_1);
        }
        break;
      }
    }
  }

  static constexpr INDEX_T sentinel = std::numeric_limits<INDEX_T>::max();
  for (auto& pair : extra) {
    size_t index = input.hash_policy_.index_for_hash(
        input.hasher_(pair.first), input.num_slots_minus_one_);
    while (true) {
      auto item = lf.indices_.get(index);
      if (gbp::BufferBlock::Ref<index_key_item<INDEX_T>>(item).index ==
          sentinel) {
        empty_value_1.index = pair.second;
        empty_value_1.key = pair.first;
        lf.indices_.set(index, &(empty_value_1));
        break;
      }
      index = (index + 1) % input.num_slots_minus_one_;
    }
  }

  lf.dump_meta(filename + ".meta");
}
#endif
}  // namespace gs

#endif  // GRAPHSCOPE_GRAPH_ID_INDEXER_H_
