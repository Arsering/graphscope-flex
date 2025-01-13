#pragma once

#include <bits/stdint-intn.h>
#include <tbb/concurrent_unordered_map.h>
#include <atomic>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <string>
#include <type_traits>
#include "base_indexer.h"
#include "flex/storages/rt_mutable_graph/types.h"

namespace gs {
#define INSERT_WITH_PARENT_OID_ENABLE false

template <typename INDEX_T, size_t SIZE>
class GroupedParentLFIndexer : public BaseIndexer<INDEX_T> {
 public:
  GroupedParentLFIndexer() : num_elements_(0), hasher_() {}
  GroupedParentLFIndexer(GroupedParentLFIndexer&& rhs)
      : keys_(std::move(rhs.keys_)),
        indices_(std::move(rhs.indices_)),
        num_elements_(rhs.num_elements_.load()),
        num_slots_minus_one_(rhs.num_slots_minus_one_),
        hasher_(rhs.hasher_) {
    hash_policy_.set_mod_function_by_index(
        rhs.hash_policy_.get_mod_function_index());
  }
  GroupedParentLFIndexer(const IdIndexer<int64_t, INDEX_T>& input,
                         const std::string& filename, double rate) {
    assert(false);
  }
  ~GroupedParentLFIndexer() override {
    LOG(INFO) << "GroupedParentLFIndexer: destructor called";
    dump_meta(file_path_ + ".meta");
  }

  size_t size() const override { return num_elements_.load(); }

  INDEX_T insert(int64_t oid) override {
    INDEX_T ind = static_cast<INDEX_T>(num_elements_.fetch_add(1));
    {
      auto item1 = keys_.get(ind);
      gbp::BufferBlock::UpdateContent<int64_t>(
          [&](int64_t& item) { item = oid; }, item1);
    }
    size_t index =
        hash_policy_.index_for_hash(hasher_(oid), num_slots_minus_one_);
    static constexpr INDEX_T sentinel = std::numeric_limits<INDEX_T>::max();

    int mark = 0;
    size_t num_get =
        indices_.OBJ_NUM_PERPAGE - index % indices_.OBJ_NUM_PERPAGE;
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
    return ind;
  }

  INDEX_T insert_with_parent_oid(int64_t oid, int64_t parent_oid,
                                 INDEX_T& previous_child_vid) override {
    LOG(FATAL) << "GroupedParentLFIndexer: insert_with_parent_oid is not "
                  "implemented";
    return 0;
  }
  size_t get_group_start() const override { return size(); }

  gbp::BufferBlock get_child_cur_start_index(size_t child_label_id,
                                             int64_t parent_oid) override {
#if ASSERT_ENABLE
    assert(child_label_id < SIZE);
#endif

    INDEX_T index = get_index(parent_oid);
    return child_indices_[child_label_id]->get(index);
  }

  INDEX_T get_index(int64_t oid) const override {
    size_t index =
        hash_policy_.index_for_hash(hasher_(oid), num_slots_minus_one_);
    static constexpr INDEX_T sentinel = std::numeric_limits<INDEX_T>::max();

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
        if (ind.key == oid) {
          return ind.index;
        }
      }
      index = (index + 1) % num_slots_minus_one_;
    }
  }

  bool get_index(int64_t oid, INDEX_T& ret) const override {
    size_t index =
        hash_policy_.index_for_hash(hasher_(oid), num_slots_minus_one_);
    static constexpr INDEX_T sentinel = std::numeric_limits<INDEX_T>::max();

    size_t num_get =
        indices_.OBJ_NUM_PERPAGE - index % indices_.OBJ_NUM_PERPAGE;
    // num_get =
    //     num_get > indices_.size() - index ? indices_.size() - index :
    //     num_get;
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
        return false;
      } else {
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

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) {
    keys_.open(snapshot_dir + "/" + name + ".keys", true);
    keys_.touch(work_dir + "/" + name + ".keys");
    indices_.open(snapshot_dir + "/" + name + ".indices", true);
    indices_.touch(work_dir + "/" + name + ".indices");
    for (size_t i = 0; i < SIZE; i++) {  // 初始化每个child_index
      child_indices_.push_back(new mmap_array<value_with_lock<INDEX_T>>());
      child_indices_[i]->open(snapshot_dir + "/" + name + "_" +
                                  std::to_string(i) + ".child_indices",
                              true);
      child_indices_[i]->touch(work_dir + "/" + name + "_" + std::to_string(i) +
                               ".child_indices");
    }

    indices_size_ = indices_.size();
    for (size_t k = keys_.size() - 1; k >= 0; --k) {
      auto item = keys_.get(k);
      if (gbp::BufferBlock::Ref<int64_t>(item) !=
          std::numeric_limits<int64_t>::max()) {
        num_elements_.store(k + 1);
        break;
      }
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
    std::string class_name_this = "GroupedParentLFIndexer";
    grape::InArchive arc;
    arc << class_name_this << num_slots_minus_one_
        << hash_policy_.get_mod_function_index();

    FILE* fout = fopen(filename.c_str(), "wb");
    fwrite(arc.GetBuffer(), sizeof(char), arc.GetSize(), fout);
    fflush(fout);
    fclose(fout);
  }

  void load_meta(const std::string& filename) {
    std::string class_name_this = "GroupedParentLFIndexer";
    grape::OutArchive arc;
    FILE* fin = fopen(filename.c_str(), "r");

    size_t meta_file_size = std::filesystem::file_size(filename);

    std::vector<char> buf(meta_file_size);
    CHECK_EQ(fread(buf.data(), sizeof(char), meta_file_size, fin),
             meta_file_size);
    arc.SetSlice(buf.data(), meta_file_size);
    size_t mod_function_index;
    std::string class_name;
    arc >> class_name >> num_slots_minus_one_ >> mod_function_index;
    if (class_name != class_name_this) {
      LOG(FATAL) << "class name mismatch: " << class_name << " vs "
                 << class_name_this;
    }
    hash_policy_.set_mod_function_by_index(mod_function_index);
  }

  // get keys
  const mmap_array<int64_t>& get_keys() const { return keys_; }

 private:
  mmap_array<int64_t> keys_;
  mmap_array<index_key_item<INDEX_T>>
      indices_;  // size() == indices_size_ == num_slots_minus_one_
                 // +log(num_slots_minus_one_)
  std::vector<mmap_array<value_with_lock<INDEX_T>>*> child_indices_;
  std::atomic<size_t> num_elements_;
  size_t num_slots_minus_one_;
  size_t indices_size_;
  ska::ska::prime_number_hash_policy hash_policy_;
  GHash<int64_t> hasher_;

  std::string file_path_;

  template <typename _INDEX_T, size_t _SIZE>
  friend void build_grouped_parent_lf_indexer(
      const IdIndexer<int64_t, _INDEX_T>& input, const std::string& filename,
      GroupedParentLFIndexer<_INDEX_T, _SIZE>& output, double rate);
};

template <typename INDEX_T>
class GroupedChildLFIndexer : public BaseIndexer<INDEX_T> {
 public:
  GroupedChildLFIndexer() : num_elements_(0), hasher_() {}
  GroupedChildLFIndexer(GroupedChildLFIndexer&& rhs)
      : keys_(std::move(rhs.keys_)),
        indices_(std::move(rhs.indices_)),
        num_elements_(rhs.num_elements_.load()),
        num_slots_minus_one_(rhs.num_slots_minus_one_),
        hasher_(rhs.hasher_) {
    hash_policy_.set_mod_function_by_index(
        rhs.hash_policy_.get_mod_function_index());
  }
  ~GroupedChildLFIndexer() override {
    LOG(INFO) << "GroupedChildLFIndexer: destructor called";
  }

  size_t size() const override { return num_elements_.load(); }
  bool change_index(int64_t oid, INDEX_T new_ind) override {
    size_t index =
        hash_policy_.index_for_hash(hasher_(oid), num_slots_minus_one_);
    static constexpr INDEX_T sentinel = std::numeric_limits<INDEX_T>::max();

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
        if (ind.key == oid) {
          gbp::BufferBlock::UpdateContent<index_key_item<INDEX_T>>(
              [&](index_key_item<INDEX_T>& item) { item.index = new_ind; },
              items, index - start_index);
          return true;
        }
      }
      index = (index + 1) % num_slots_minus_one_;
    }
  }
  INDEX_T insert(int64_t oid) override {
    INDEX_T ind = static_cast<INDEX_T>(start_index_buffer_.fetch_add(1));
    num_elements_.fetch_add(1);
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
    size_t num_get =
        indices_.OBJ_NUM_PERPAGE - index % indices_.OBJ_NUM_PERPAGE;
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
    return ind;
  }
#if INSERT_WITH_PARENT_OID_ENABLE
  // 使用parent_oid插入
  INDEX_T insert_with_parent_oid(int64_t oid, int64_t parent_oid,
                                 INDEX_T& previous_child_vid) override {
    const auto ind =
        get_new_child_index_private(oid, parent_oid, previous_child_vid);
    num_elements_.fetch_add(1);

    size_t index =
        hash_policy_.index_for_hash(hasher_(oid), num_slots_minus_one_);
    static constexpr INDEX_T sentinel = std::numeric_limits<INDEX_T>::max();

    int mark = 0;
    size_t num_get =
        indices_.OBJ_NUM_PERPAGE - index % indices_.OBJ_NUM_PERPAGE;
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

    return ind;
  }

  INDEX_T get_new_child_index_private(int64_t oid, int64_t parent_oid,
                                      INDEX_T& previous_child_vid) override {
    INDEX_T ret;
    INDEX_T ind_start, new_ind_start;
    bool require_update = false;
    gbp::BufferBlock key_items;
    size_t group_size;
    auto grouped_index_item = parent_indexer_->get_child_cur_start_index(
        kid_label_id_in_parent_, parent_oid);
    {
      gbp::BufferBlock::OperateContentAtomic<value_with_lock<INDEX_T>>(
          [&](value_with_lock<INDEX_T>& item) {
            bool lock_state;
            do {
              lock_state = false;
            } while (!item.lock.compare_exchange_weak(
                lock_state, true, std::memory_order_release,
                std::memory_order_relaxed));  // 自旋等待锁释放
            ind_start = item.value;
          },
          grouped_index_item);
      if (unlikely(ind_start == value_with_lock<INDEX_T>::EMPTY_VALUE.value)) {
        ind_start =
            __sync_fetch_and_add(&group_config_.first, group_config_.second);
        new_ind_start = ind_start;
        // LOG(INFO) << "update new_ind_start: " << new_ind_start
        //           << " oid: " << oid;
        require_update = true;
      }

      key_items = keys_.get(ind_start, group_config_.second);  // 获取key_items
      gbp::BufferBlock::UpdateContent<int64_t>(
          [&](int64_t& item) {
            if (unlikely(require_update)) {
              item = ind_start;  // 更新item
            } else if ((item + 1) % group_config_.second == 0) {
              new_ind_start = __sync_fetch_and_add(&group_config_.first,
                                                   group_config_.second);
              // LOG(INFO) << "update new_ind_start: " << new_ind_start
              //           << " oid: " << oid;
              require_update = true;

              auto new_key_item =
                  keys_.get(new_ind_start + group_config_.second - 1);
              gbp::BufferBlock::UpdateContent<int64_t>(
                  [&](int64_t& item) {
                    item = new_ind_start;  // 更新item
                  },
                  new_key_item);
            }
            ret = item++;  // 更新ret
          },
          key_items, group_config_.second - 1);

      if (unlikely(require_update)) {
        gbp::BufferBlock::UpdateContent<value_with_lock<INDEX_T>>(
            [&](value_with_lock<INDEX_T>& item) {
              item.value = new_ind_start;
              item.lock.store(false, std::memory_order_relaxed);  // 释放锁
            },
            grouped_index_item);

      } else {
        gbp::BufferBlock::OperateContentAtomic<value_with_lock<INDEX_T>>(
            [&](value_with_lock<INDEX_T>& item) {
              item.lock.store(false, std::memory_order_relaxed);  // 释放锁
            },
            grouped_index_item);
      }
    }
    gbp::BufferBlock::UpdateContent<int64_t>([&](int64_t& item) { item = oid; },
                                             key_items, ret - ind_start);

    return ret;
  }
  size_t get_group_start() const override { return group_config_.first; }

#else
  size_t get_group_start() const override { return group_config_.first; }

  // 不使用parent_oid插入
  INDEX_T insert_with_parent_oid(int64_t oid, int64_t parent_oid_,
                                 INDEX_T& previous_child_vid) override {
    INDEX_T ind = static_cast<INDEX_T>(num_elements_.fetch_add(1));
    {
      auto key_item = keys_.get(ind);
      gbp::BufferBlock::UpdateContent<int64_t>(
          [&](int64_t& item) { item = oid; }, key_item);
    }
    size_t index =
        hash_policy_.index_for_hash(hasher_(oid), num_slots_minus_one_);
    static constexpr INDEX_T sentinel = std::numeric_limits<INDEX_T>::max();

    int mark = 0;
    size_t num_get =
        indices_.OBJ_NUM_PERPAGE - index % indices_.OBJ_NUM_PERPAGE;
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
    return ind;
  }
#endif

  bool set_parent_lf(BaseIndexer<INDEX_T>& parent_lf) override {
    parent_indexer_ = &parent_lf;
    return true;
  }

  INDEX_T get_index(int64_t oid) const override {
    size_t index =
        hash_policy_.index_for_hash(hasher_(oid), num_slots_minus_one_);
    static constexpr INDEX_T sentinel = std::numeric_limits<INDEX_T>::max();

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
  }

  bool get_index(int64_t oid, INDEX_T& ret) const override {
    size_t index =
        hash_policy_.index_for_hash(hasher_(oid), num_slots_minus_one_);
    static constexpr INDEX_T sentinel = std::numeric_limits<INDEX_T>::max();

    size_t num_get =
        indices_.OBJ_NUM_PERPAGE - index % indices_.OBJ_NUM_PERPAGE;
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
        return false;
      } else {
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

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) {
    keys_.open(snapshot_dir + "/" + name + ".keys", true);
    keys_.touch(work_dir + "/" + name + ".keys");
    indices_.open(snapshot_dir + "/" + name + ".indices", true);
    indices_.touch(work_dir + "/" + name + ".indices");

    indices_size_ = indices_.size();
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
    std::string class_name_this = "GroupedChildLFIndexer";
    grape::InArchive arc;
    arc << class_name_this << num_slots_minus_one_
        << hash_policy_.get_mod_function_index() << num_elements_.load()
        << kid_label_id_in_parent_ << group_config_
        << start_index_buffer_.load() << buffer_capacity_;
    FILE* fout = fopen(filename.c_str(), "wb");
    fwrite(arc.GetBuffer(), sizeof(char), arc.GetSize(), fout);
    fflush(fout);
    fclose(fout);
  }

  void load_meta(const std::string& filename) {
    std::string class_name_this = "GroupedChildLFIndexer";
    grape::OutArchive arc;
    FILE* fin = fopen(filename.c_str(), "r");

    size_t meta_file_size = std::filesystem::file_size(filename);

    std::vector<char> buf(meta_file_size);
    CHECK_EQ(fread(buf.data(), sizeof(char), meta_file_size, fin),
             meta_file_size);
    arc.SetSlice(buf.data(), meta_file_size);
    size_t mod_function_index;
    std::string class_name;
    size_t num_elements;
    size_t start_index_buffer;
    arc >> class_name >> num_slots_minus_one_ >> mod_function_index >>
        num_elements >> kid_label_id_in_parent_ >> group_config_ >>
        start_index_buffer >> buffer_capacity_;
    start_index_buffer_ = start_index_buffer;
    num_elements_ = num_elements;
    if (class_name != class_name_this) {
      LOG(FATAL) << "class name mismatch: " << class_name << " vs "
                 << class_name_this;
    }
    hash_policy_.set_mod_function_by_index(mod_function_index);
  }

  // get keys
  const mmap_array<int64_t>& get_keys() const { return keys_; }

 private:
  mmap_array<int64_t> keys_;
  mmap_array<index_key_item<INDEX_T>>
      indices_;  // size() == indices_size_ == num_slots_minus_one_
                 // +log(num_slots_minus_one_)
  std::atomic<size_t> num_elements_;
  size_t num_slots_minus_one_;
  size_t indices_size_;
  ska::ska::prime_number_hash_policy hash_policy_;
  GHash<int64_t> hasher_;

  BaseIndexer<INDEX_T>* parent_indexer_;
  int64_t kid_label_id_in_parent_;
  std::pair<int64_t, INDEX_T> group_config_;

  std::atomic<size_t> start_index_buffer_;
  size_t buffer_capacity_;

  template <typename _INDEX_T>
  friend void build_grouped_child_lf_indexer(
      const IdIndexer<int64_t, _INDEX_T>& input, const std::string& filename,
      GroupedChildLFIndexer<_INDEX_T>& output, size_t label_id_in_parent,
      size_t group_size, double rate);
  template <typename _INDEX_T>
  friend void build_grouped_child_lf_indexer_new(
      const IdIndexer<int64_t, _INDEX_T>& input, const std::string& filename,
      GroupedChildLFIndexer<_INDEX_T>& output, size_t label_id_in_parent,
      size_t group_size, double rate);
};

template <class INDEX_T, size_t SIZE>
void build_grouped_parent_lf_indexer(const IdIndexer<int64_t, INDEX_T>& input,
                                     const std::string& filename,
                                     GroupedParentLFIndexer<INDEX_T, SIZE>& lf,
                                     double rate) {
  double indices_rate = static_cast<double>(input.keys_.size()) /
                        static_cast<double>(input.indices_.size());
  CHECK_LT(indices_rate, rate);

  size_t size = input.keys_.size();
  size_t lf_size = static_cast<double>(size) / rate + 1;
  lf_size = std::max(lf_size, static_cast<size_t>(1024));

  for (size_t i = 0; i < SIZE; i++) {  // 初始化每个child_index
    lf.child_indices_.push_back(new mmap_array<value_with_lock<INDEX_T>>());
    lf.child_indices_[i]->open(
        filename + "_" + std::to_string(i) + ".child_indices", false);
    lf.child_indices_[i]->resize(lf_size);
  }
  std::string_view value_with_lock_empty_value = {
      reinterpret_cast<const char*>(&value_with_lock<INDEX_T>::EMPTY_VALUE),
      sizeof(value_with_lock<INDEX_T>::EMPTY_VALUE)};
  lf.keys_.open(filename + ".keys", false);
  lf.keys_.resize(lf_size);
  // FIXME: input.keys_是mmap分配的还是malloc分配的？？？
  auto keys_item = lf.keys_.get(0, lf_size);
  for (size_t t = 0; t < size; t++) {
    gbp::BufferBlock::UpdateContent<int64_t>(
        [&](int64_t& item) { item = input.keys_[t]; }, keys_item, t);
    // 初始化每个child_index
    for (size_t i = 0; i < SIZE; i++) {
      lf.child_indices_[i]->set_single_obj(t, value_with_lock_empty_value);
    }
  }

  for (size_t t = size; t < lf_size; t++) {
    gbp::BufferBlock::UpdateContent<int64_t>(
        [&](int64_t& item) { item = std::numeric_limits<int64_t>::max(); },
        keys_item, t);
    // 初始化每个child_index
    for (size_t i = 0; i < SIZE; i++) {
      lf.child_indices_[i]->set_single_obj(t, value_with_lock_empty_value);
    }
  }

  lf.num_elements_.store(size);

  lf.indices_.open(filename + ".indices", false);
  lf.indices_.resize(input.indices_.size());

  index_key_item<INDEX_T> empty_value_1;
  empty_value_1.index = std::numeric_limits<INDEX_T>::max();
  empty_value_1.key = 0;

  for (size_t k = 0; k != input.indices_.size(); ++k) {
    lf.indices_.set(k, &empty_value_1);  // 初始化每个index_key_item
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
          lf.indices_.set_single_obj(
              index, {reinterpret_cast<const char*>(&empty_value_1),
                      sizeof(empty_value_1)});
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
        lf.indices_.set_single_obj(
            index, {reinterpret_cast<const char*>(&empty_value_1),
                    sizeof(empty_value_1)});
        break;
      }
      index = (index + 1) % input.num_slots_minus_one_;
    }
  }
  lf.file_path_ = filename;
}

template <typename INDEX_T>
void build_grouped_child_lf_indexer(const IdIndexer<int64_t, INDEX_T>& input,
                                    const std::string& filename,
                                    GroupedChildLFIndexer<INDEX_T>& lf,
                                    size_t label_id_in_parent,
                                    size_t group_size, double rate) {
  const size_t start_index = 0;

  lf.start_index_buffer_ = 0;
  lf.buffer_capacity_ = start_index;

  lf.kid_label_id_in_parent_ = label_id_in_parent;
  lf.group_config_ = {(input.keys_.size() + start_index + group_size - 1) /
                          group_size * group_size,
                      group_size};

  double indices_rate = static_cast<double>(input.keys_.size()) /
                        static_cast<double>(input.indices_.size());
  CHECK_LT(indices_rate, rate);

  size_t size = input.keys_.size();
  size_t lf_size = static_cast<double>(size) / rate + 1;
  lf_size = std::max(lf_size, static_cast<size_t>(1024)) + start_index;

  lf.keys_.open(filename + ".keys", false);
  lf.keys_.resize(lf_size);

  // FIXME: input.keys_是mmap分配的还是malloc分配的？？？
  auto keys_item = lf.keys_.get(0, lf_size);
  for (size_t t = 0; t < size; t++) {
    gbp::BufferBlock::UpdateContent<int64_t>(
        [&](int64_t& item) { item = input.keys_[t]; }, keys_item,
        t + start_index);
  }

  for (size_t k = 0; k != start_index; ++k) {
    gbp::BufferBlock::UpdateContent<int64_t>(
        [&](int64_t& item) { item = std::numeric_limits<int64_t>::max(); },
        keys_item, k);
  }

  for (size_t k = size + start_index; k != lf_size; ++k) {
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
          empty_value_1.index = ret + start_index;
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

template <typename INDEX_T>
void build_grouped_child_lf_indexer_new(
    const IdIndexer<int64_t, INDEX_T>& input, const std::string& filename,
    GroupedChildLFIndexer<INDEX_T>& lf, size_t label_id_in_parent,
    size_t group_size, double rate) {
  lf.kid_label_id_in_parent_ = label_id_in_parent;
  lf.group_config_ = {0, group_size};

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
  for (size_t t = 0; t < lf_size; t++) {
    gbp::BufferBlock::UpdateContent<int64_t>(
        [&](int64_t& item) { item = std::numeric_limits<int64_t>::max(); },
        keys_item, t);
  }

  lf.num_elements_.store(size);

  lf.indices_.open(filename + ".indices", false);
  lf.indices_.resize(input.indices_.size());

  index_key_item<INDEX_T> empty_value_1{std::numeric_limits<INDEX_T>::max(), 0};
  for (size_t k = 0; k != lf.indices_.size(); ++k) {
    lf.indices_.set(k, &empty_value_1);
  }
  lf.indices_size_ = lf.indices_.size();

  lf.hash_policy_.set_mod_function_by_index(
      input.hash_policy_.get_mod_function_index());
  lf.num_slots_minus_one_ = input.num_slots_minus_one_;

  lf.dump_meta(filename + ".meta");
}

inline BaseIndexer<vid_t>* GroupedParentLFIndexer_creation_helper(
    IdIndexer<oid_t, vid_t>& indexer, const std::string& prefix,
    size_t group_size) {
  switch (group_size) {
  case 1: {
    GroupedParentLFIndexer<vid_t, 1>* lf_indexer =
        new GroupedParentLFIndexer<vid_t, 1>();
    build_grouped_parent_lf_indexer(indexer, prefix, *lf_indexer);
    return lf_indexer;
  }
  case 2: {
    GroupedParentLFIndexer<gs::vid_t, 2>* lf_indexer =
        new GroupedParentLFIndexer<gs::vid_t, 2>();
    build_grouped_parent_lf_indexer(indexer, prefix, *lf_indexer);
    return lf_indexer;
  }
  case 3: {
    GroupedParentLFIndexer<vid_t, 3>* lf_indexer =
        new GroupedParentLFIndexer<vid_t, 3>();
    build_grouped_parent_lf_indexer(indexer, prefix, *lf_indexer);
    return lf_indexer;
  }
  case 4: {
    GroupedParentLFIndexer<vid_t, 4>* lf_indexer =
        new GroupedParentLFIndexer<vid_t, 4>();
    build_grouped_parent_lf_indexer(indexer, prefix, *lf_indexer);
    return lf_indexer;
  }
  default:
    assert(false);
    return nullptr;
  }
}

}  // namespace gs