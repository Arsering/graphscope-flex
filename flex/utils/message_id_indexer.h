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
// 泛型类，OID是object ID ，VID是Vertex ID
template <typename OID = int64_t, typename INDEX_T = int64_t>
class MessageIdAllocator : public gs::BaseIndexer<INDEX_T> {
  // 确保OID和LID是整数类型
  static_assert(std::is_integral<OID>::value, "OID must be an integral type");
  static_assert(std::is_integral<INDEX_T>::value,
                "VID must be an integral type");

 private:
  static constexpr int MESSAGE_IN_BLOCK = 3;  // 每个block中可以存放的消息数量

  tbb::concurrent_unordered_map<OID, int>
      person_counters;                     // 每个person的当前计数器
  std::atomic<int> global_block_count{0};  // 全局block计数器

  // 存储每个message id对应的person
  tbb::concurrent_unordered_map<INDEX_T, OID> message_owners;

  // 存储外部消息ID到内部消息ID的映射
  tbb::concurrent_unordered_map<OID, INDEX_T> oid_to_lid_map;

  // 存储内部消息ID到外部消息ID的映射
  tbb::concurrent_unordered_map<INDEX_T, OID> lid_to_oid_map;

 public:
  MessageIdAllocator() = default;

  // 获取下一个消息ID，现在需要传入外部消息ID
  INDEX_T get_message_id(OID person, OID message_oid) {
    // 检查这个oid是否已经分配过lid
    auto oid_it = oid_to_lid_map.find(message_oid);
    if (oid_it != oid_to_lid_map.end()) {
      return oid_it->second;  // 如果已经分配过，直接返回对应的lid
    }

    // 获取该person当前的计数器值
    auto it = person_counters.find(person);
    if (it == person_counters.end()) {
      // 新的person，分配一个新的block
      person_counters.insert(
          {person, global_block_count.fetch_add(1) * MESSAGE_IN_BLOCK});
      it = person_counters.find(person);
    }

    // 计算当前消息的全局offset
    INDEX_T current_offset = static_cast<INDEX_T>(it->second);

    // 记录message id的所有者
    message_owners.insert({current_offset, person});

    // 记录oid到lid的映射
    oid_to_lid_map.insert({message_oid, current_offset});
    lid_to_oid_map.insert({current_offset, message_oid});

    // 增加计数器
    int new_counter = it->second + 1;
    person_counters[person] = new_counter;

    // 检查是否需要分配新的block
    if (new_counter % MESSAGE_IN_BLOCK == 0) {
      // 分配新的block
      person_counters[person] =
          global_block_count.fetch_add(1) * MESSAGE_IN_BLOCK;
    }

    return current_offset;
  }

  // 根据外部消息ID获取内部消息ID
  bool get_lid_by_oid(OID message_oid, INDEX_T& lid) const {
    auto it = oid_to_lid_map.find(message_oid);
    if (it == oid_to_lid_map.end()) {
      return false;
    }
    lid = it->second;
    return true;
  }

  // 根据内部消息ID获取外部消息ID
  bool get_oid_by_lid(INDEX_T message_lid, OID& oid) const {
    auto it = lid_to_oid_map.find(message_lid);
    if (it == lid_to_oid_map.end()) {
      return false;
    }
    oid = it->second;
    return true;
  }

  // 获取当前全局block数量
  int get_block_count() const { return global_block_count.load(); }

  // 打印block分配情况
  void print_block_allocation() const {
    std::cout << "\n=== Block Allocation Status ===\n";

    int total_blocks = get_block_count();
    for (int block = 0; block < total_blocks; ++block) {
      INDEX_T block_start = static_cast<INDEX_T>(block * MESSAGE_IN_BLOCK);
      std::cout << "\nBlock " << block << " (Message IDs " << block_start
                << " - " << (block_start + MESSAGE_IN_BLOCK - 1) << "):\n";

      // 检查这个block中的每个message id
      for (int offset = 0; offset < MESSAGE_IN_BLOCK; ++offset) {
        INDEX_T msg_id = block_start + offset;
        auto it = message_owners.find(msg_id);
        if (it != message_owners.end()) {
          std::cout << "  Message ID " << std::setw(3) << msg_id
                    << " -> Person " << it->second;

          // 尝试找到对应的OID
          auto oid_it = lid_to_oid_map.find(msg_id);
          if (oid_it != lid_to_oid_map.end()) {
            std::cout << " (OID: " << oid_it->second << ")";
          }
          std::cout << "\n";
        } else {
          std::cout << "  Message ID " << std::setw(3) << msg_id
                    << " -> Unallocated\n";
        }
      }
    }
    std::cout << "\nTotal Blocks: " << total_blocks << "\n";
    std::cout << "=========================\n";
  }
  // 将当前状态保存到文件
  void dump(const std::string& file_path) const {
    std::ofstream file(file_path, std::ios::binary);
    if (!file) {
      throw std::runtime_error("Cannot open file for writing: " + file_path);
    }

    // 保存 block 数量
    int block_count = global_block_count.load();
    file.write(reinterpret_cast<const char*>(&block_count),
               sizeof(block_count));

    // 保存 person_counters
    size_t counter_size = person_counters.size();
    file.write(reinterpret_cast<const char*>(&counter_size),
               sizeof(counter_size));
    for (const auto& pair : person_counters) {
      file.write(reinterpret_cast<const char*>(&pair.first),
                 sizeof(pair.first));
      file.write(reinterpret_cast<const char*>(&pair.second),
                 sizeof(pair.second));
    }

    // 保存 message_owners
    size_t owners_size = message_owners.size();
    file.write(reinterpret_cast<const char*>(&owners_size),
               sizeof(owners_size));
    for (const auto& pair : message_owners) {
      file.write(reinterpret_cast<const char*>(&pair.first),
                 sizeof(pair.first));
      file.write(reinterpret_cast<const char*>(&pair.second),
                 sizeof(pair.second));
    }

    // 保存 oid_to_lid_map
    size_t oid_map_size = oid_to_lid_map.size();
    file.write(reinterpret_cast<const char*>(&oid_map_size),
               sizeof(oid_map_size));
    for (const auto& pair : oid_to_lid_map) {
      file.write(reinterpret_cast<const char*>(&pair.first),
                 sizeof(pair.first));
      file.write(reinterpret_cast<const char*>(&pair.second),
                 sizeof(pair.second));
    }

    // 保存 lid_to_oid_map
    size_t lid_map_size = lid_to_oid_map.size();
    file.write(reinterpret_cast<const char*>(&lid_map_size),
               sizeof(lid_map_size));
    for (const auto& pair : lid_to_oid_map) {
      file.write(reinterpret_cast<const char*>(&pair.first),
                 sizeof(pair.first));
      file.write(reinterpret_cast<const char*>(&pair.second),
                 sizeof(pair.second));
    }
  }

  // 从文件加载状态
  void load(const std::string& file_path) {
    std::ifstream file(file_path, std::ios::binary);
    if (!file) {
      throw std::runtime_error("Cannot open file for reading: " + file_path);
    }

    // 清除现有状态
    person_counters.clear();
    message_owners.clear();
    oid_to_lid_map.clear();
    lid_to_oid_map.clear();

    // 加载 block 数量
    int block_count;
    file.read(reinterpret_cast<char*>(&block_count), sizeof(block_count));
    global_block_count.store(block_count);

    // 加载 person_counters
    size_t counter_size;
    file.read(reinterpret_cast<char*>(&counter_size), sizeof(counter_size));
    for (size_t i = 0; i < counter_size; ++i) {
      OID first;
      int second;
      file.read(reinterpret_cast<char*>(&first), sizeof(first));
      file.read(reinterpret_cast<char*>(&second), sizeof(second));
      person_counters[first] = second;
    }

    // 加载 message_owners
    size_t owners_size;
    file.read(reinterpret_cast<char*>(&owners_size), sizeof(owners_size));
    for (size_t i = 0; i < owners_size; ++i) {
      INDEX_T first;
      OID second;
      file.read(reinterpret_cast<char*>(&first), sizeof(first));
      file.read(reinterpret_cast<char*>(&second), sizeof(second));
      message_owners[first] = second;
    }

    // 加载 oid_to_lid_map
    size_t oid_map_size;
    file.read(reinterpret_cast<char*>(&oid_map_size), sizeof(oid_map_size));
    for (size_t i = 0; i < oid_map_size; ++i) {
      OID first;
      INDEX_T second;
      file.read(reinterpret_cast<char*>(&first), sizeof(first));
      file.read(reinterpret_cast<char*>(&second), sizeof(second));
      oid_to_lid_map[first] = second;
    }

    // 加载 lid_to_oid_map
    size_t lid_map_size;
    file.read(reinterpret_cast<char*>(&lid_map_size), sizeof(lid_map_size));
    for (size_t i = 0; i < lid_map_size; ++i) {
      INDEX_T first;
      OID second;
      file.read(reinterpret_cast<char*>(&first), sizeof(first));
      file.read(reinterpret_cast<char*>(&second), sizeof(second));
      lid_to_oid_map[first] = second;
    }
  }

  // 实现基类接口
  bool get_index(OID oid, INDEX_T& lid) const override {
    return get_lid_by_oid(oid, lid);
  }

  OID get_key(const INDEX_T& lid) const override {
    OID oid;
    if (!get_oid_by_lid(lid, oid)) {
      throw std::runtime_error("LID not found");
    }
    return oid;
  }

  size_t size() const override {
    return global_block_count.load() * MESSAGE_IN_BLOCK;
  }

  void dump(const std::string& name, const std::string& snapshot_dir) override {
    dump(snapshot_dir + "/" + name);
  }

  // 添加缺失的虚函数实现
  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) override {
    load(snapshot_dir + "/" + name);
  }
};

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

      gbp::BufferBlock::UpdateContent<index_key_item_grouped<INDEX_T, SIZE>>(
          [&](index_key_item_grouped<INDEX_T, SIZE>& item) {
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

  INDEX_T insert_with_parent_oid(int64_t oid, int64_t parent_oid) override {
    LOG(FATAL) << "GroupedParentLFIndexer: insert_with_parent_oid is not "
                  "implemented";
    return 0;
  }
  op_status get_kid_index(size_t kid_label_id, int64_t parent_oid,
                          INDEX_T& ret) override {
#if ASSERT_ENABLE
    assert(kid_label_id < SIZE);
    INDEX_T parent_vid = 0;
    assert(get_index(parent_oid, parent_vid));  // 判断parent是否存在
#endif
    auto [items, index] = get_extra_values(parent_oid, kid_label_id);
    gbp::BufferBlock::UpdateContent<index_key_item_grouped<INDEX_T, SIZE>>(
        [&](index_key_item_grouped<INDEX_T, SIZE>& item) {
          bool lock_state = false;
          while (item.extra_values[kid_label_id].lock.compare_exchange_weak(
              lock_state, true, std::memory_order_release,
              std::memory_order_relaxed))
            ;  // 自旋等待锁释放
          if (item.extra_values[kid_label_id].value ==
              std::numeric_limits<INDEX_T>::max() >> 1) {  // 尚未初始化
            item.extra_values[kid_label_id].value =
                gbp::as_atomic(group_configs_[kid_label_id].first)
                    .fetch_add(group_configs_[kid_label_id].second);
          }

          ret = item.extra_values[kid_label_id].value++;
          if (item.extra_values[kid_label_id].value %
                  group_configs_[kid_label_id].second ==
              0) {
            item.extra_values[kid_label_id].value =
                gbp::as_atomic(group_configs_[kid_label_id].first)
                    .fetch_add(group_configs_[kid_label_id].second);
          }
          item.extra_values[kid_label_id].lock.store(
              false,
              std::memory_order_relaxed);  // 释放锁
        },
        items, index);
    return op_status::op_status_success;
  }
  op_status set_new_kid_range(size_t kid_label_id,
                              int64_t max_kid_oid) override {
#if ASSERT_ENABLE
    assert(kid_label_id < SIZE);
#endif
    group_configs_[kid_label_id].first =
        (max_kid_oid + group_configs_[kid_label_id].second - 1) /
        group_configs_[kid_label_id].second *
        group_configs_[kid_label_id].second;
    LOG(INFO) << "max_kid_oid: " << max_kid_oid
              << " group_configs_[kid_label_id].second: "
              << group_configs_[kid_label_id].second
              << " group_configs_[kid_label_id].first: "
              << group_configs_[kid_label_id].first;
    return op_status::op_status_success;
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

      auto& ind = gbp::BufferBlock::Ref<index_key_item_grouped<INDEX_T, SIZE>>(
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

      auto& ind = gbp::BufferBlock::Ref<index_key_item_grouped<INDEX_T, SIZE>>(
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
    for (size_t i = 0; i < SIZE; ++i) {
      arc << group_configs_[i].first << group_configs_[i].second;
    }
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
    for (size_t i = 0; i < SIZE; ++i) {
      arc >> group_configs_[i].first >> group_configs_[i].second;
      LOG(INFO) << "group_configs_[i].first: " << group_configs_[i].first
                << " group_configs_[i].second: " << group_configs_[i].second;
    }
    if (class_name != class_name_this) {
      LOG(FATAL) << "class name mismatch: " << class_name << " vs "
                 << class_name_this;
    }
    hash_policy_.set_mod_function_by_index(mod_function_index);
  }

  // get keys
  const mmap_array<int64_t>& get_keys() const { return keys_; }

 private:
  // pair<gbp::BufferBlock, item_index_in_buffer>
  std::pair<gbp::BufferBlock, size_t> get_extra_values(
      int64_t oid, size_t kid_label_id) const {
#if ASSERT_ENABLE
    assert(kid_label_id < SIZE);
#endif
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

      auto& ind = gbp::BufferBlock::Ref<index_key_item_grouped<INDEX_T, SIZE>>(
          items, index - start_index);
      if (ind.index == sentinel) {
        LOG(FATAL) << "cannot find " << oid << " in id_indexer";
      } else {
        if (ind.key == oid) {
          return {items, index - start_index};
        }
      }
      index = (index + 1) % num_slots_minus_one_;
    }
  }

  mmap_array<int64_t> keys_;
  mmap_array<index_key_item_grouped<INDEX_T, SIZE>>
      indices_;  // size() == indices_size_ == num_slots_minus_one_
                 // +log(num_slots_minus_one_)
  std::atomic<size_t> num_elements_;
  size_t num_slots_minus_one_;
  size_t indices_size_;
  ska::ska::prime_number_hash_policy hash_policy_;
  GHash<int64_t> hasher_;

  std::array<std::pair<int64_t, int64_t>, SIZE>
      group_configs_;  // pair<max_vertex_id, group_size>

  template <typename _INDEX_T, size_t _SIZE>
  friend void build_grouped_parent_lf_indexer(
      const IdIndexer<int64_t, _INDEX_T>& input, const std::string& filename,
      GroupedParentLFIndexer<_INDEX_T, _SIZE>& output,
      std::vector<size_t>& group_sizes, double rate);
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

  size_t size() const override { return num_elements_.load(); }

  INDEX_T insert(int64_t oid) override {
    LOG(FATAL) << "GroupedChildLFIndexer: insert is not implemented and "
                  "foreign_oid is required";
    return 0;
  }

  INDEX_T insert_with_parent_oid(int64_t oid, int64_t parent_oid) override {
    INDEX_T ind;
    assert(parent_indexer_->get_kid_index(kid_label_id_in_parent_, parent_oid,
                                          ind) == op_status::op_status_success);
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
    return ind;
  }
  op_status get_kid_index(size_t kid_label_id, int64_t parent_oid,
                          INDEX_T& ret) override {
    LOG(FATAL) << "GroupedChildLFIndexer: get_kid_index is not implemented";
    return op_status::op_status_not_implemented;
  }
  op_status set_new_kid_range(size_t kid_label_id,
                              int64_t max_kid_oid) override {
    LOG(FATAL) << "GroupedChildLFIndexer: set_new_kid_range is not implemented";
    return op_status::op_status_not_implemented;
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
    std::string class_name_this = "GroupedChildLFIndexer";
    grape::InArchive arc;
    arc << class_name_this << num_slots_minus_one_
        << hash_policy_.get_mod_function_index();
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
  std::atomic<size_t> num_elements_;
  size_t num_slots_minus_one_;
  size_t indices_size_;
  ska::ska::prime_number_hash_policy hash_policy_;
  GHash<int64_t> hasher_;

  BaseIndexer<INDEX_T>* parent_indexer_;
  int64_t kid_label_id_in_parent_;

  template <typename _INDEX_T>
  friend void build_grouped_child_lf_indexer(
      const IdIndexer<int64_t, _INDEX_T>& input, const std::string& filename,
      GroupedChildLFIndexer<_INDEX_T>& output, BaseIndexer<_INDEX_T>& parent_lf,
      size_t label_id_in_parent, double rate);
};

template <class INDEX_T, size_t SIZE>
void build_grouped_parent_lf_indexer(const IdIndexer<int64_t, INDEX_T>& input,
                                     const std::string& filename,
                                     GroupedParentLFIndexer<INDEX_T, SIZE>& lf,
                                     std::vector<size_t>& group_sizes,
                                     double rate) {
  double indices_rate = static_cast<double>(input.keys_.size()) /
                        static_cast<double>(input.indices_.size());
  CHECK_LT(indices_rate, rate);

  size_t size = input.keys_.size();
  size_t lf_size = static_cast<double>(size) / rate + 1;
  lf_size = std::max(lf_size, static_cast<size_t>(1024));
  assert(group_sizes.size() == lf.group_configs_.size());

  for (auto group_id = 0; group_id < lf.group_configs_.size(); group_id++) {
    lf.group_configs_[group_id].first = 0;
    lf.group_configs_[group_id].second = group_sizes[group_id];
  }

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

  index_key_item_grouped<INDEX_T, SIZE> empty_value_1;
  empty_value_1.index = std::numeric_limits<INDEX_T>::max();
  empty_value_1.key = 0;
  for (size_t i = 0; i < empty_value_1.extra_values.size(); ++i) {
    empty_value_1.extra_values[i].value =
        std::numeric_limits<INDEX_T>::max() >> 1;
    empty_value_1.extra_values[i].lock.store(false, std::memory_order_relaxed);
  }
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
      if (gbp::BufferBlock::Ref<index_key_item_grouped<INDEX_T, SIZE>>(item)
              .index == sentinel) {
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

  lf.dump_meta(filename + ".meta");
}

template <typename INDEX_T>
void build_grouped_child_lf_indexer(const IdIndexer<int64_t, INDEX_T>& input,
                                    const std::string& filename,
                                    GroupedChildLFIndexer<INDEX_T>& lf,
                                    BaseIndexer<INDEX_T>& parent_lf,
                                    size_t label_id_in_parent, double rate) {
  lf.parent_indexer_ = &parent_lf;
  lf.kid_label_id_in_parent_ = label_id_in_parent;

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

inline BaseIndexer<vid_t>* GroupedParentLFIndexer_creation_helper(
    IdIndexer<oid_t, vid_t>& indexer, const std::string& prefix,
    std::vector<size_t>& group_sizes) {
  switch (group_sizes.size()) {
  case 1: {
    GroupedParentLFIndexer<vid_t, 1>* lf_indexer =
        new GroupedParentLFIndexer<vid_t, 1>();
    build_grouped_parent_lf_indexer(indexer, prefix, *lf_indexer, group_sizes);
    return lf_indexer;
  }
  case 2: {
    GroupedParentLFIndexer<gs::vid_t, 2>* lf_indexer =
        new GroupedParentLFIndexer<gs::vid_t, 2>();
    build_grouped_parent_lf_indexer(indexer, prefix, *lf_indexer, group_sizes);
    return lf_indexer;
  }
  case 3: {
    GroupedParentLFIndexer<vid_t, 3>* lf_indexer =
        new GroupedParentLFIndexer<vid_t, 3>();
    build_grouped_parent_lf_indexer(indexer, prefix, *lf_indexer, group_sizes);
    return lf_indexer;
  }
  case 4: {
    GroupedParentLFIndexer<vid_t, 4>* lf_indexer =
        new GroupedParentLFIndexer<vid_t, 4>();
    build_grouped_parent_lf_indexer(indexer, prefix, *lf_indexer, group_sizes);
    return lf_indexer;
  }
  default:
    assert(false);
    return nullptr;
  }
}

}  // namespace gs