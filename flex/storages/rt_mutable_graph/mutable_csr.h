/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef GRAPHSCOPE_GRAPH_MUTABLE_CSR_H_
#define GRAPHSCOPE_GRAPH_MUTABLE_CSR_H_

#include <atomic>
#include <filesystem>
#include <type_traits>
#include <vector>

#include "flex/storages/rt_mutable_graph/types.h"
#include "flex/utils/allocators.h"
#include "flex/utils/mmap_array.h"
#include "flex/utils/property/types.h"
#include "grape/serialization/in_archive.h"
#include "grape/serialization/out_archive.h"
#include "grape/utils/concurrent_queue.h"

namespace gs {

template <typename EDATA_T>
struct MutableNbr {
  MutableNbr() = default;
  MutableNbr(const MutableNbr& rhs)
      : neighbor(rhs.neighbor),
        timestamp(rhs.timestamp.load()),
        data(rhs.data) {}
  ~MutableNbr() = default;

  vid_t neighbor;
  std::atomic<timestamp_t> timestamp;
  EDATA_T data;
};  // namespace gstemplate<typenameEDATA_T>struct MutableNbr

template <>
struct MutableNbr<grape::EmptyType> {
  MutableNbr() = default;
  MutableNbr(const MutableNbr& rhs)
      : neighbor(rhs.neighbor), timestamp(rhs.timestamp.load()) {}
  ~MutableNbr() = default;

  vid_t neighbor;
  union {
    std::atomic<timestamp_t> timestamp;
    grape::EmptyType data;
  };
};

#if OV
template <typename EDATA_T>
class MutableNbrSlice {
 public:
  using nbr_t = MutableNbr<EDATA_T>;
  MutableNbrSlice() = default;
  ~MutableNbrSlice() = default;

  void set_size(int size) { size_ = size; }
  int size() const { return size_; }

  void set_begin(const nbr_t* ptr) { ptr_ = ptr; }

  const nbr_t* begin() const { return ptr_; }
  const nbr_t* end() const { return ptr_ + size_; }

  static MutableNbrSlice empty() {
    MutableNbrSlice ret;
    ret.set_begin(nullptr);
    ret.set_size(0);
    return ret;
  }

 private:
  const nbr_t* ptr_;
  int size_;
};

template <typename EDATA_T>
class MutableNbrSliceMut {
 public:
  using nbr_t = MutableNbr<EDATA_T>;
  MutableNbrSliceMut() = default;
  ~MutableNbrSliceMut() = default;

  void set_size(int size) { size_ = size; }
  int size() const { return size_; }

  void set_begin(nbr_t* ptr) { ptr_ = ptr; }

  nbr_t* begin() { return ptr_; }
  nbr_t* end() { return ptr_ + size_; }

  static MutableNbrSliceMut empty() {
    MutableNbrSliceMut ret;
    ret.set_begin(nullptr);
    ret.set_size(0);
    return ret;
  }

 private:
  nbr_t* ptr_;
  int size_;
};
#else
template <typename EDATA_T>
class MutableNbrSlice {
 public:
  using nbr_t = MutableNbr<EDATA_T>;
  MutableNbrSlice() = default;
  ~MutableNbrSlice() = default;

  void set_size(int size) { size_ = size; }
  int size() const { return size_; }
  size_t get_size() const { return size_; }
  void set_mmap_array(const mmap_array<nbr_t>* ma) { mmap_array_ = ma; }
  const mmap_array<nbr_t>* get_mmap_array() const { return mmap_array_; }

  void set_buffer(const nbr_t* buffer) { buffer_ = buffer; }
  const nbr_t* get_buffer() const { return buffer_; }

  void set_start_idx(size_t start_idx) { start_idx_ = start_idx; }
  size_t get_start_idx() const { return start_idx_; }

  static MutableNbrSlice empty() {
    MutableNbrSlice ret;
    ret.set_mmap_array(nullptr);
    ret.set_buffer(nullptr);
    ret.set_start_idx(0);
    ret.set_size(0);
    return ret;
  }

 private:
  const mmap_array<nbr_t>* mmap_array_;
  const nbr_t* buffer_;
  size_t start_idx_;
  size_t cur_idx_;
  size_t size_;
};

template <typename EDATA_T>
class MutableNbrSliceMut {
 public:
  using nbr_t = MutableNbr<EDATA_T>;
  MutableNbrSliceMut() = default;
  ~MutableNbrSliceMut() = default;

  void set_size(int size) { size_ = size; }
  int size() const { return size_; }

  void set_mmap_array(mmap_array<nbr_t>* ma) { mmap_array_ = ma; }
  mmap_array<nbr_t>* get_mmap_array() const { return mmap_array_; }

  void set_buffer(nbr_t* buffer) { buffer_ = buffer; }
  nbr_t* get_buffer() const { return buffer_; }

  void set_start_idx(size_t start_idx) { start_idx_ = start_idx; }
  size_t get_start_idx() const { return start_idx_; }

  static MutableNbrSliceMut empty() {
    MutableNbrSliceMut ret;
    ret.set_mmap_array(nullptr);
    ret.set_buffer(nullptr);
    ret.set_start_idx(0);
    ret.set_size(0);
    return ret;
  }

 private:
  mmap_array<nbr_t>* mmap_array_;
  nbr_t* buffer_;
  size_t start_idx_;
  size_t cur_idx_;
  size_t size_;
};
#endif
template <typename T>
struct UninitializedUtils {
  static void copy(T* new_buffer, T* old_buffer, size_t len) {
    memcpy(new_buffer, old_buffer, len * sizeof(T));
  }
};
#if OV
template <typename EDATA_T>
class MutableAdjlist {
 public:
  using nbr_t = MutableNbr<EDATA_T>;
  using slice_t = MutableNbrSlice<EDATA_T>;
  using mut_slice_t = MutableNbrSliceMut<EDATA_T>;
  MutableAdjlist() : buffer_(NULL), size_(0), capacity_(0) {}
  ~MutableAdjlist() {}

  void init(nbr_t* ptr, int cap, int size) {
    buffer_ = ptr;
    capacity_ = cap;
    size_ = size;
  }

  void batch_put_edge(vid_t neighbor, const EDATA_T& data, timestamp_t ts = 0) {
    CHECK_LT(size_, capacity_);
    auto& nbr = buffer_[size_++];
    nbr.neighbor = neighbor;
    nbr.data = data;
    nbr.timestamp.store(ts);
  }

  void put_edge(vid_t neighbor, const EDATA_T& data, timestamp_t ts,
                MMapAllocator& allocator) {
    if (size_ == capacity_) {
      capacity_ += ((capacity_) >> 1);
      capacity_ = std::max(capacity_, 8);
      nbr_t* new_buffer =
          static_cast<nbr_t*>(allocator.allocate(capacity_ * sizeof(nbr_t)));
      if (size_ > 0) {
        UninitializedUtils<nbr_t>::copy(new_buffer, buffer_, size_);
      }
      buffer_ = new_buffer;
    }
    auto& nbr = buffer_[size_.fetch_add(1)];
    nbr.neighbor = neighbor;
    nbr.data = data;
    nbr.timestamp.store(ts);
  }

  slice_t get_edges() const {
    slice_t ret;
    ret.set_size(size_.load(std::memory_order_acquire));
    ret.set_begin(buffer_);
    return ret;
  }

  mut_slice_t get_edges_mut() {
    mut_slice_t ret;
    ret.set_size(size_.load());
    ret.set_begin(buffer_);
    return ret;
  }

  int capacity() const { return capacity_; }
  int size() const { return size_; }
  const nbr_t* data() const { return buffer_; }
  nbr_t* data() { return buffer_; }

 private:
  nbr_t* buffer_;
  std::atomic<int> size_;
  int capacity_;
};
#else

template <typename EDATA_T>
class MutableAdjlist {
 public:
  using nbr_t = MutableNbr<EDATA_T>;
  using slice_t = MutableNbrSlice<EDATA_T>;
  using mut_slice_t = MutableNbrSliceMut<EDATA_T>;

  MutableAdjlist()
      : mmap_array_(nullptr),
        buffer_(nullptr),
        is_buffer_(false),
        start_idx_(0),
        size_(0),
        capacity_(0) {}
  ~MutableAdjlist() {}

  void init(mmap_array<nbr_t>* ma, size_t start_idx, size_t cap, size_t size) {
    buffer_ = nullptr;
    is_buffer_ = false;
    size_ = size;
    capacity_ = cap;
    mmap_array_ = ma;
    start_idx_ = start_idx;
  }

  bool is_buffer() const { return is_buffer_; }

  void batch_put_edge(vid_t neighbor, const EDATA_T& data, timestamp_t ts = 0) {
    CHECK_LT(size_, capacity_);
    if (is_buffer_) {
      auto& nbr = buffer_[size_++];
      nbr.neighbor = neighbor;
      nbr.data = data;
      nbr.timestamp.store(ts);
    } else {
      nbr_t nbr;
      nbr.neighbor = neighbor;
      nbr.data = data;
      nbr.timestamp.store(ts);
      mmap_array_->set(size_ + start_idx_, nbr);
      size_++;
    }
  }

  void put_edge(vid_t neighbor, const EDATA_T& data, timestamp_t ts,
                MMapAllocator& allocator) {
    if (size_ == capacity_) {
      capacity_ += ((capacity_) >> 1);
      capacity_ = std::max(capacity_, 8);
      nbr_t* new_buffer =
          static_cast<nbr_t*>(allocator.allocate(capacity_ * sizeof(nbr_t)));
      if (size_ > 0) {
        UninitializedUtils<nbr_t>::copy(new_buffer, buffer_, size_);
      }
      buffer_ = new_buffer;
      is_buffer_ = true;
    }
    batch_put_edge(neighbor, data, ts);
  }

  slice_t get_edges() const {
    slice_t ret;
    ret.set_mmap_array(mmap_array_);
    ret.set_buffer(buffer_);
    ret.set_start_idx(start_idx_);
    ret.set_size(size_.load(std::memory_order_acquire));
    return ret;
  }

  mut_slice_t get_edges_mut() const {
    mut_slice_t ret;
    ret.set_mmap_array(mmap_array_);
    ret.set_buffer(buffer_);
    ret.set_start_idx(start_idx_);
    ret.set_size(size_.load());
    return ret;
  }

  nbr_t get_edge(size_t idx) {
    if (is_buffer_) {
      return buffer_[idx];
    } else {
      auto item = mmap_array_->get(idx + start_idx_);
      return gbp::Decode<nbr_t>(item);
    }
  }

  int capacity() const { return capacity_; }
  int size() const { return size_; }
  const nbr_t* get_buffer() const { return buffer_; }
  nbr_t* get_buffer() { return buffer_; }
  const mmap_array<nbr_t>* get_mmap_array() const { return mmap_array_; }
  mmap_array<nbr_t>* get_mmap_array() { return mmap_array_; }

 private:
  nbr_t* buffer_ = nullptr;
  std::atomic<int> size_;
  int capacity_;
  // NBRs nbrs_;
  bool is_buffer_;
  mmap_array<nbr_t>* mmap_array_ = nullptr;
  size_t start_idx_;
};
#endif

#if OV
class MutableCsrConstEdgeIterBase {
 public:
  MutableCsrConstEdgeIterBase() = default;
  virtual ~MutableCsrConstEdgeIterBase() = default;

  virtual vid_t get_neighbor() const = 0;
  virtual Any get_data() const = 0;
  virtual timestamp_t get_timestamp() const = 0;
  virtual size_t size() const = 0;

  virtual void next() = 0;
  virtual bool is_valid() const = 0;
};

class MutableCsrEdgeIterBase {
 public:
  MutableCsrEdgeIterBase() = default;
  virtual ~MutableCsrEdgeIterBase() = default;

  virtual vid_t get_neighbor() const = 0;
  virtual Any get_data() const = 0;
  virtual timestamp_t get_timestamp() const = 0;
  virtual void set_data(const Any& value, timestamp_t ts) = 0;

  virtual void next() = 0;
  virtual bool is_valid() const = 0;
};
#else
class MutableCsrConstEdgeIterBase {
 public:
  MutableCsrConstEdgeIterBase() = default;
  virtual ~MutableCsrConstEdgeIterBase() = default;

  virtual vid_t get_neighbor() = 0;
  virtual gbp::BufferObject get_data() = 0;
  virtual timestamp_t get_timestamp() = 0;
  virtual size_t size() const = 0;

  virtual void next() = 0;
  virtual bool is_valid() const = 0;
};

class MutableCsrEdgeIterBase {
 public:
  MutableCsrEdgeIterBase() = default;
  virtual ~MutableCsrEdgeIterBase() = default;

  virtual vid_t get_neighbor() = 0;
  virtual gbp::BufferObject get_data() = 0;
  virtual timestamp_t get_timestamp() = 0;
  // virtual void set_data(const gbp::BufferObject& value, timestamp_t ts) = 0;
  virtual void set_data(const Any& value, timestamp_t ts) = 0;

  virtual void next() = 0;
  virtual bool is_valid() const = 0;
};
#endif

class MutableCsrBase {
 public:
  MutableCsrBase() {}
  virtual ~MutableCsrBase() {}

  virtual void batch_init(const std::string& name, const std::string& work_dir,
                          const std::vector<int>& degree) = 0;

  virtual void open(const std::string& name, const std::string& snapshot_dir,
                    const std::string& work_dir) = 0;

  virtual void dump(const std::string& name,
                    const std::string& new_spanshot_dir) = 0;

  virtual void resize(vid_t vnum) = 0;
  virtual size_t size() const = 0;

  virtual void put_generic_edge(vid_t src, vid_t dst, const Any& data,
                                timestamp_t ts, MMapAllocator& alloc) = 0;

  virtual void ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc,
                           timestamp_t ts, MMapAllocator& alloc) = 0;
  virtual void peek_ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc,
                                timestamp_t ts, MMapAllocator& alloc) = 0;

  virtual std::shared_ptr<MutableCsrConstEdgeIterBase> edge_iter(
      vid_t v) const = 0;

  virtual MutableCsrConstEdgeIterBase* edge_iter_raw(vid_t v) const = 0;

  virtual std::shared_ptr<MutableCsrEdgeIterBase> edge_iter_mut(vid_t v) = 0;

  virtual size_t get_index_size_in_byte() const = 0;

  virtual size_t get_data_size_in_byte() const = 0;
};

#if OV
template <typename EDATA_T>
class TypedMutableCsrConstEdgeIter : public MutableCsrConstEdgeIterBase {
  using nbr_t = MutableNbr<EDATA_T>;

 public:
  explicit TypedMutableCsrConstEdgeIter(const MutableNbrSlice<EDATA_T>& slice)
      : cur_(slice.begin()), end_(slice.end()) {}
  ~TypedMutableCsrConstEdgeIter() = default;

  vid_t get_neighbor() const { return cur_->neighbor; }
  Any get_data() const { return AnyConverter<EDATA_T>::to_any(cur_->data); }
  timestamp_t get_timestamp() const { return cur_->timestamp.load(); }

  void next() { ++cur_; }
  bool is_valid() const { return cur_ != end_; }
  size_t size() const { return end_ - cur_; }

 private:
  const nbr_t* cur_;
  const nbr_t* end_;
};

template <typename EDATA_T>
class TypedMutableCsrEdgeIter : public MutableCsrEdgeIterBase {
  using nbr_t = MutableNbr<EDATA_T>;

 public:
  explicit TypedMutableCsrEdgeIter(MutableNbrSliceMut<EDATA_T> slice)
      : cur_(slice.begin()), end_(slice.end()) {}
  ~TypedMutableCsrEdgeIter() = default;

  vid_t get_neighbor() const { return cur_->neighbor; }
  Any get_data() const { return AnyConverter<EDATA_T>::to_any(cur_->data); }
  timestamp_t get_timestamp() const { return cur_->timestamp.load(); }

  void set_data(const Any& value, timestamp_t ts) {
    ConvertAny<EDATA_T>::to(value, cur_->data);
    cur_->timestamp.store(ts);
  }

  void next() { ++cur_; }
  bool is_valid() const { return cur_ != end_; }

 private:
  nbr_t* cur_;
  nbr_t* end_;
};
#else
template <typename EDATA_T>
class TypedMutableCsrConstEdgeIter : public MutableCsrConstEdgeIterBase {
  using nbr_t = MutableNbr<EDATA_T>;

 public:
  TypedMutableCsrConstEdgeIter()
      : mmap_array_(nullptr),
        buffer_(nullptr),
        start_idx_(0),
        cur_idx_(0),
        size_(0) {}
  explicit TypedMutableCsrConstEdgeIter(const MutableNbrSlice<EDATA_T>& slice)
      : mmap_array_(slice.get_mmap_array()),
        buffer_(slice.get_buffer()),
        start_idx_(slice.get_start_idx()),
        cur_idx_(start_idx_),
        size_(slice.size()) {
    cur_idx_--;
    next();
  }
  explicit TypedMutableCsrConstEdgeIter(const mmap_array<nbr_t>* ma,
                                        size_t start_idx, size_t size)
      : mmap_array_(ma),
        buffer_(nullptr),
        start_idx_(start_idx),
        cur_idx_(start_idx_),
        size_(size) {
    cur_idx_--;
    next();
  }
  explicit TypedMutableCsrConstEdgeIter(const nbr_t* buffer, size_t start_idx,
                                        size_t size)
      : mmap_array_(nullptr),
        buffer_(buffer),
        start_idx_(start_idx),
        cur_idx_(start_idx_),
        size_(size) {}
  ~TypedMutableCsrConstEdgeIter() = default;

  vid_t get_neighbor() {
    if (buffer_ == nullptr) {
      if (unlikely(is_valid() && !fresh_)) {
        cur_obj_ = mmap_array_->get(cur_idx_);
        fresh_ = true;
      }
      return gbp::Decode<nbr_t>(cur_obj_).neighbor;
    } else {
      return buffer_[cur_idx_].neighbor;
    }
  }

  gbp::BufferObject get_data() {
    gbp::BufferObject buf(sizeof(EDATA_T));
    if (buffer_ == nullptr) {
      if (unlikely(is_valid() && !fresh_)) {
        cur_obj_ = mmap_array_->get(cur_idx_);
        fresh_ = true;
      }
      memcpy(buf.Data(), &(gbp::Decode<nbr_t>(cur_obj_).data), sizeof(EDATA_T));
    } else {
      memcpy(buf.Data(), &(buffer_[cur_idx_].data), sizeof(EDATA_T));
    }
    return buf;
  }

  timestamp_t get_timestamp() {
    if (buffer_ == nullptr) {
      if (likely(is_valid() && !fresh_)) {
        cur_obj_ = mmap_array_->get(cur_idx_);
        fresh_ = true;
      }
      return gbp::Decode<nbr_t>(cur_obj_).timestamp.load();
    } else {
      return buffer_[cur_idx_].timestamp.load();
    }
  }

  void next() {
    ++cur_idx_;
    fresh_ = false;
  }

  bool is_valid() const { return cur_idx_ < start_idx_ + size_; }
  size_t size() const { return size_; }

  void set_cur(size_t idx) {
    CHECK_LT(idx, start_idx_ + size_);
    CHECK_LE(start_idx_, idx);
    cur_idx_ = idx;
  }

 private:
  const mmap_array<nbr_t>* mmap_array_;
  const nbr_t* buffer_;
  size_t start_idx_;
  size_t cur_idx_;
  gbp::BufferObject cur_obj_;
  bool fresh_ = false;
  size_t size_;
};

template <typename EDATA_T>
class TypedMutableCsrEdgeIter : public MutableCsrEdgeIterBase {
  using nbr_t = MutableNbr<EDATA_T>;

 public:
  TypedMutableCsrEdgeIter()
      : mmap_array_(nullptr),
        buffer_(nullptr),
        start_idx_(0),
        cur_idx_(0),
        size_(0) {}
  explicit TypedMutableCsrEdgeIter(MutableNbrSliceMut<EDATA_T> slice)
      : mmap_array_(slice.get_mmap_array()),
        buffer_(slice.get_buffer()),
        start_idx_(slice.get_start_idx()),
        cur_idx_(start_idx_),
        size_(slice.size()) {}
  explicit TypedMutableCsrEdgeIter(mmap_array<nbr_t>* ma, size_t start_idx,
                                   size_t size)
      : mmap_array_(ma),
        buffer_(nullptr),
        start_idx_(start_idx),
        cur_idx_(start_idx_),
        size_(size) {}
  explicit TypedMutableCsrEdgeIter(nbr_t* buffer, size_t start_idx, size_t size)
      : mmap_array_(nullptr),
        buffer_(buffer),
        start_idx_(start_idx),
        cur_idx_(start_idx_),
        size_(size) {}
  ~TypedMutableCsrEdgeIter() = default;

  vid_t get_neighbor() {
    if (buffer_ == nullptr) {
      if (unlikely(is_valid() && !fresh_)) {
        cur_obj_ = mmap_array_->get(cur_idx_);
        fresh_ = true;
      }
      return gbp::Decode<nbr_t>(cur_obj_).neighbor;
    } else {
      return buffer_[cur_idx_].neighbor;
    }
  }

  gbp::BufferObject get_data() {
    gbp::BufferObject buf(sizeof(EDATA_T));
    if (buffer_ == nullptr) {
      if (unlikely(is_valid() && !fresh_)) {
        cur_obj_ = mmap_array_->get(cur_idx_);
        fresh_ = true;
      }
      memcpy(buf.Data(), &(gbp::Decode<nbr_t>(cur_obj_).data), sizeof(EDATA_T));
    } else {
      memcpy(buf.Data(), &(buffer_[cur_idx_].data), sizeof(EDATA_T));
    }
    return buf;
  }

  timestamp_t get_timestamp() {
    if (buffer_ == nullptr) {
      if (likely(is_valid() && !fresh_)) {
        cur_obj_ = mmap_array_->get(cur_idx_);
        fresh_ = true;
      }
      return gbp::Decode<nbr_t>(cur_obj_).timestamp.load();
    } else {
      return buffer_[cur_idx_].timestamp.load();
    }
  }

  void set_data(const Any& value, timestamp_t ts) {
    if (buffer_ == nullptr) {
      auto item = mmap_array_->get(cur_idx_);
      auto item_obj = gbp::Decode<nbr_t>(item);
      ConvertAny<EDATA_T>::to(value, item_obj.data);
      item_obj.timestamp.store(ts);
      mmap_array_->set(cur_idx_, item);
    } else {
      auto& buf = buffer_[cur_idx_];
      ConvertAny<EDATA_T>::to(value, buf.data);
      buf.timestamp.store(ts);
    }
  }

  void next() {
    ++cur_idx_;
    fresh_ = false;
  }
  void set_cur(size_t idx) {
    CHECK_LT(idx, start_idx_ + size_);
    CHECK_LE(start_idx_, idx);
    cur_idx_ = idx;
  }
  bool is_valid() const { return cur_idx_ < start_idx_ + size_; }

 private:
  mmap_array<nbr_t>* mmap_array_;
  nbr_t* buffer_;
  size_t start_idx_;
  size_t cur_idx_;
  gbp::BufferObject cur_obj_;
  bool fresh_ = false;
  size_t size_;
};
#endif

template <typename EDATA_T>
class TypedMutableCsrBase : public MutableCsrBase {
 public:
  using slice_t = MutableNbrSlice<EDATA_T>;
  virtual void batch_put_edge(vid_t src, vid_t dst, const EDATA_T& data,
                              timestamp_t ts = 0) = 0;

  virtual slice_t get_edges(vid_t i) const = 0;
};

template <typename EDATA_T>
class MutableCsr : public TypedMutableCsrBase<EDATA_T> {
 public:
  using nbr_t = MutableNbr<EDATA_T>;
  using adjlist_t = MutableAdjlist<EDATA_T>;
  using slice_t = MutableNbrSlice<EDATA_T>;
  using mut_slice_t = MutableNbrSliceMut<EDATA_T>;

  MutableCsr() : locks_(nullptr) {}
  ~MutableCsr() {
    if (locks_ != nullptr) {
      delete[] locks_;
    }
  }

  void batch_init(const std::string& name, const std::string& work_dir,
                  const std::vector<int>& degree) override {
    size_t vnum = degree.size();
    adj_lists_.open(work_dir + "/" + name + ".adj", false);
    adj_lists_.resize(vnum);

    locks_ = new grape::SpinLock[vnum];

    size_t edge_num = 0;
    for (auto d : degree) {
      edge_num += d;
    }
    nbr_list_.open(work_dir + "/" + name + ".nbr", false);
    nbr_list_.resize(edge_num);
#if OV
    nbr_t* ptr = nbr_list_.data();
    for (vid_t i = 0; i < vnum; ++i) {
      int deg = degree[i];
      adj_lists_[i].init(ptr, deg, 0);
      ptr += deg;
    }
#else
    size_t offset = 0;
    for (vid_t i = 0; i < vnum; ++i) {
      int deg = degree[i];
      auto adj_list = gbp::BufferObject::Copy(adj_lists_.get(i));
      gbp::Decode<adjlist_t>(adj_list).init(&nbr_list_, offset, deg, 0);
      adj_lists_.set(i, adj_list);
      offset += deg;
    }
#endif
  }

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) override {
    LOG(INFO) << "MutableCsr";
    mmap_array<int> degree_list;
    degree_list.open(snapshot_dir + "/" + name + ".deg", true);
    nbr_list_.open(snapshot_dir + "/" + name + ".nbr", true);
    adj_lists_.open(work_dir + "/" + name + ".adj", false);

    adj_lists_.resize(degree_list.size());
    locks_ = new grape::SpinLock[degree_list.size()];
    LOG(INFO) << " degree_list.size() = " << degree_list.size();
    signed long int t1, t2, t3;
    size_t sum = 0;
    size_t sum2 = 0;
    gbp::get_time_duration_g(0) = 0;
    gbp::get_time_duration_g(1) = 0;

    t1 = -gbp::GetSystemTime();
#if OV
    nbr_t* ptr = nbr_list_.data();
    for (size_t i = 0; i < degree_list.size(); ++i) {
      t2 = -gbp::GetSystemTime();
      int degree = degree_list[i];
      t2 += gbp::GetSystemTime();
      sum += t2;
      // LOG(INFO) << "read degree (s) = " << t2;
      // t3 = -grape::GetCurrentTime();
      adj_lists_[i].init(ptr, degree, degree);
      // t3 += grape::GetCurrentTime();
      // sum2 += t3;
      ptr += degree;
    }
#else
    size_t offset = 0;
    for (size_t i = 0; i < degree_list.size(); ++i) {
      t2 = -gbp::GetSystemTime();
      auto item = degree_list.get(
          i);  // 此操作占本函数40%的latency，且当i=1时的latency占整个本行代码latency的约20%
      t2 += gbp::GetSystemTime();
      sum += t2;

      int degree = gbp::Decode<int>(item);
      auto adj_list = gbp::BufferObject(sizeof(adjlist_t));
      // auto adj_list = gbp::BufferObject::Copy(adj_lists_.get(i));
      gbp::Decode<adjlist_t>(adj_list).init(&nbr_list_, offset, degree, degree);
      // t3 = -grape::GetCurrentTime();
      adj_lists_.set(i, adj_list);  // 此操作占了本函数latency的另外40%
      // t3 += grape::GetCurrentTime();
      // sum2 += t3;
      // LOG(INFO) << "init adjlist (s) = " << t3;
      offset += degree;
    }
#endif
    t1 += gbp::GetSystemTime();
    LOG(INFO) << "sum  (CPU cycle) = " << sum;
    LOG(INFO) << "Fetch Page (CPU cycle) = " << gbp::get_time_duration_g(0);
    LOG(INFO) << "Copy Object (CPU cycle) = " << gbp::get_time_duration_g(1);
    // LOG(INFO) << "sum2  (s) = " << sum2;
    LOG(INFO) << "adjlist init (s) = " << t1;
  }

  void dump(const std::string& name,
            const std::string& new_spanshot_dir) override {
    size_t vnum = adj_lists_.size();
    bool reuse_nbr_list = true;
    mmap_array<int> degree_list;
    degree_list.open(new_spanshot_dir + "/" + name + ".deg", false);
    degree_list.resize(vnum);
#if OV
    size_t offset = 0;
    for (size_t i = 0; i < vnum; ++i) {
      if (adj_lists_[i].size() != 0) {
        if (!(adj_lists_[i].data() == nbr_list_.data() + offset &&
              offset < nbr_list_.size())) {
          reuse_nbr_list = false;
        }
      }
      degree_list[i] = adj_lists_[i].size();
      offset += degree_list[i];
    }
#else
    size_t offset = 0;
    size_t size_tmp = 0;
    for (size_t i = 0; i < vnum; ++i) {
      size_tmp = adj_lists_.get(i).Size();
      if (size_tmp != 0) {
        auto adj_list = adj_lists_.get(i);
        if (gbp::Decode<adjlist_t>(adj_list).get_mmap_array() == nullptr) {
          reuse_nbr_list = false;
        }
      }
      degree_list.set(i, size_tmp);
      offset += size_tmp;
    }
#endif

    if (reuse_nbr_list && !nbr_list_.filename().empty() &&
        std::filesystem::exists(nbr_list_.filename())) {
      std::filesystem::create_hard_link(nbr_list_.filename(),
                                        new_spanshot_dir + "/" + name + ".nbr");
    } else {
      FILE* fout =
          fopen((new_spanshot_dir + "/" + name + ".nbr").c_str(), "wb");
#if OV
      for (size_t i = 0; i < vnum; ++i) {
        fwrite(adj_lists_[i].data(), sizeof(nbr_t), adj_lists_[i].size(), fout);
      }
#else
      for (size_t i = 0; i < vnum; ++i) {
        nbr_t* buffer;
        auto adj_list = adj_lists_.get(i);
        auto& adj_list_obj = gbp::Decode<adjlist_t>(adj_list);
        if ((buffer = adj_list_obj.get_buffer()) != nullptr) {
          fwrite(buffer, sizeof(nbr_t), adj_list_obj.size(), fout);
        } else {
          for (size_t j = 0; j < adj_list_obj.size(); j++) {
            auto item = adj_list_obj.get_edge(j);
            fwrite(&item, sizeof(nbr_t), 1, fout);
          }
        }
      }
#endif

      fflush(fout);
      fclose(fout);
    }
  }

  void resize(vid_t vnum) override {
    if (vnum > adj_lists_.size()) {
      size_t old_size = adj_lists_.size();
      adj_lists_.resize(vnum);
      for (size_t k = old_size; k != vnum; ++k) {
#if OV
        adj_lists_[k].init(NULL, 0, 0);
#else
        auto adj_list = gbp::BufferObject::Copy(adj_lists_.get(k));
        gbp::Decode<adjlist_t>(adj_list).init(nullptr, 0, 0, 0);
        adj_lists_.set(k, adj_list);
#endif
      }
      delete[] locks_;
      locks_ = new grape::SpinLock[vnum];
    } else {
      adj_lists_.resize(vnum);
    }
  }

  size_t size() const override { return adj_lists_.size(); }
#if OV
  void batch_put_edge(vid_t src, vid_t dst, const EDATA_T& data,
                      timestamp_t ts = 0) override {
    adj_lists_[src].batch_put_edge(dst, data, ts);
  }
#else
  void batch_put_edge(vid_t src, vid_t dst, const EDATA_T& data,
                      timestamp_t ts = 0) override {
    auto adj_list = adj_lists_.get(src);
    gbp::Decode<adjlist_t>(adj_list).batch_put_edge(dst, data, ts);
  }
#endif
  void put_generic_edge(vid_t src, vid_t dst, const Any& data, timestamp_t ts,
                        MMapAllocator& alloc) override {
    EDATA_T value;
    ConvertAny<EDATA_T>::to(data, value);
    put_edge(src, dst, value, ts, alloc);
  }
#if OV
  void put_edge(vid_t src, vid_t dst, const EDATA_T& data, timestamp_t ts,
                MMapAllocator& alloc) {
    CHECK_LT(src, adj_lists_.size());
    locks_[src].lock();
    adj_lists_[src].put_edge(dst, data, ts, alloc);
    locks_[src].unlock();
  }
#else
  void put_edge(vid_t src, vid_t dst, const EDATA_T& data, timestamp_t ts,
                MMapAllocator& alloc) {
    CHECK_LT(src, adj_lists_.size());
    locks_[src].lock();
    auto adj_list = gbp::BufferObject::Copy(adj_lists_.get(src));
    gbp::Decode<adjlist_t>(adj_list).put_edge(dst, data, ts, alloc);
    adj_lists_.set(src, adj_list);
    locks_[src].unlock();
  }
#endif

#if OV
  int degree(vid_t i) const { return adj_lists_[i].size(); }
  slice_t get_edges(vid_t i) const override {
    return adj_lists_[i].get_edges();
  }
  mut_slice_t get_edges_mut(vid_t i) { return adj_lists_[i].get_edges_mut(); }
#else
  int degree(vid_t i) const {
    auto adj_list = adj_lists_.get(i);
    return gbp::Decode<adjlist_t>(adj_list).size();
  }
  slice_t get_edges(vid_t i) const override {
    auto item = adj_lists_.get(i);
    return gbp::Decode<adjlist_t>(item).get_edges();
  }
  mut_slice_t get_edges_mut(vid_t i) {
    auto item = adj_lists_.get(i);
    return gbp::Decode<adjlist_t>(item).get_edges_mut();
  }

#endif
  size_t get_index_size_in_byte() const override {
    return adj_lists_.get_size_in_byte();
  }
  size_t get_data_size_in_byte() const override {
    return nbr_list_.get_size_in_byte();
  }

  void ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc, timestamp_t ts,
                   MMapAllocator& alloc) override {
    EDATA_T value;
    arc >> value;
    put_edge(src, dst, value, ts, alloc);
  }

  void peek_ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc,
                        timestamp_t ts, MMapAllocator& alloc) override {
    EDATA_T value;
    arc.Peek<EDATA_T>(value);
    put_edge(src, dst, value, ts, alloc);
  }

  std::shared_ptr<MutableCsrConstEdgeIterBase> edge_iter(
      vid_t v) const override {
    return std::make_shared<TypedMutableCsrConstEdgeIter<EDATA_T>>(
        get_edges(v));
  }

  MutableCsrConstEdgeIterBase* edge_iter_raw(vid_t v) const override {
    return new TypedMutableCsrConstEdgeIter<EDATA_T>(get_edges(v));
  }
  std::shared_ptr<MutableCsrEdgeIterBase> edge_iter_mut(vid_t v) override {
    return std::make_shared<TypedMutableCsrEdgeIter<EDATA_T>>(get_edges_mut(v));
  }

 private:
  grape::SpinLock* locks_;
  mmap_array<adjlist_t> adj_lists_;
  mmap_array<nbr_t> nbr_list_;
};

template <typename EDATA_T>
class SingleMutableCsr : public TypedMutableCsrBase<EDATA_T> {
 public:
  using nbr_t = MutableNbr<EDATA_T>;
  using slice_t = MutableNbrSlice<EDATA_T>;
  using mut_slice_t = MutableNbrSliceMut<EDATA_T>;

  SingleMutableCsr() {}
  ~SingleMutableCsr() {}

  void batch_init(const std::string& name, const std::string& work_dir,
                  const std::vector<int>& degree) override {
    size_t vnum = degree.size();
    nbr_list_.open(work_dir + "/" + name + ".nbr", false);
    nbr_list_.resize(vnum);
#if OV
    for (size_t k = 0; k != vnum; ++k) {
      nbr_list_[k].timestamp.store(std::numeric_limits<timestamp_t>::max());
    }
#else
    // FIXME: store/load在这里是没有意义的！！！
    for (size_t k = 0; k != vnum; ++k) {
      auto item = nbr_list_.get(k);
      gbp::Decode<nbr_t>(item).timestamp.store(
          std::numeric_limits<timestamp_t>::max());
      nbr_list_.set(k, item);
    }
#endif
  }

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) {
    LOG(INFO) << " singleMutableCsr ";
    nbr_list_.open(snapshot_dir + "/" + name + ".nbr", true);
    nbr_list_.touch(work_dir + "/" + name + ".nbr");
  }

  void dump(const std::string& name,
            const std::string& new_snapshot_dir) override {
    assert(!nbr_list_.filename().empty() &&
           std::filesystem::exists(nbr_list_.filename()));
    assert(!nbr_list_.read_only());
    std::filesystem::create_hard_link(nbr_list_.filename(),
                                      new_snapshot_dir + "/" + name + ".nbr");
  }

  void resize(vid_t vnum) override {
    if (vnum > nbr_list_.size()) {
      size_t old_size = nbr_list_.size();
      nbr_list_.resize(vnum);
      for (size_t k = old_size; k != vnum; ++k) {
#if OV
        nbr_list_[k].timestamp.store(std::numeric_limits<timestamp_t>::max());
#else
        // FIXME:
        auto item = nbr_list_.get(k);
        gbp::Decode<nbr_t>(item).timestamp.store(
            std::numeric_limits<timestamp_t>::max());
        nbr_list_.set(k, item);
#endif
      }
    } else {
      nbr_list_.resize(vnum);
    }
  }

  size_t size() const override { return nbr_list_.size(); }
#if OV
  void batch_put_edge(vid_t src, vid_t dst, const EDATA_T& data,
                      timestamp_t ts = 0) override {
    nbr_list_[src].neighbor = dst;
    nbr_list_[src].data = data;
    CHECK_EQ(nbr_list_[src].timestamp.load(),
             std::numeric_limits<timestamp_t>::max());
    nbr_list_[src].timestamp.store(ts);
  }
#else
  void batch_put_edge(vid_t src, vid_t dst, const EDATA_T& data,
                      timestamp_t ts = 0) override {
    nbr_t buf;
    buf.neighbor = dst;
    buf.data = data;
    auto item = nbr_list_.get(src);
    CHECK_EQ(gbp::Decode<nbr_t>(item).timestamp.load(),
             std::numeric_limits<timestamp_t>::max());
    buf.timestamp.store(ts);
    nbr_list_.set(src, buf);
  }
#endif
  void put_generic_edge(vid_t src, vid_t dst, const Any& data, timestamp_t ts,
                        MMapAllocator& alloc) override {
    EDATA_T value;
    ConvertAny<EDATA_T>::to(data, value);
    put_edge(src, dst, value, ts, alloc);
  }
#if OV
  void put_edge(vid_t src, vid_t dst, const EDATA_T& data, timestamp_t ts,
                MMapAllocator&) {
    CHECK_LT(src, nbr_list_.size());
    nbr_list_[src].neighbor = dst;
    nbr_list_[src].data = data;
    CHECK_EQ(nbr_list_[src].timestamp, std::numeric_limits<timestamp_t>::max());
    nbr_list_[src].timestamp.store(ts);
  }
#else
  void put_edge(vid_t src, vid_t dst, const EDATA_T& data, timestamp_t ts,
                MMapAllocator&) {
    CHECK_LT(src, nbr_list_.size());
    nbr_t buf;
    buf.neighbor = dst;
    buf.data = data;
    auto item = nbr_list_.get(src);
    CHECK_EQ(gbp::Decode<nbr_t>(item).timestamp,
             std::numeric_limits<timestamp_t>::max());
    buf.timestamp.store(ts);
    nbr_list_.set(src, buf);
  }
#endif

#if OV
  slice_t get_edges(vid_t i) const override {
    slice_t ret;
    ret.set_size(nbr_list_[i].timestamp.load() ==
                         std::numeric_limits<timestamp_t>::max()
                     ? 0
                     : 1);
    if (ret.size() != 0) {
      ret.set_begin(&nbr_list_[i]);
    }
    return ret;
  }

  mut_slice_t get_edges_mut(vid_t i) {
    mut_slice_t ret;
    ret.set_size(nbr_list_[i].timestamp.load() ==
                         std::numeric_limits<timestamp_t>::max()
                     ? 0
                     : 1);
    if (ret.size() != 0) {
      ret.set_begin(&nbr_list_[i]);
    }
    return ret;
  }
  const nbr_t& get_edge(vid_t i) const { return nbr_list_[i]; }
#else
  slice_t get_edges(vid_t i) const override {
    slice_t ret;
    auto item = nbr_list_.get(i);
    ret.set_size(gbp::Decode<nbr_t>(item).timestamp.load() ==
                         std::numeric_limits<timestamp_t>::max()
                     ? 0
                     : 1);
    if (ret.size() != 0) {
      ret.set_mmap_array(&nbr_list_);
      ret.set_buffer(nullptr);
      ret.set_start_idx(i);
      ret.set_size(1);
    }
    return ret;
  }

  mut_slice_t get_edges_mut(vid_t i) {
    mut_slice_t ret;
    auto item = nbr_list_.get(i);
    ret.set_size(gbp::Decode<nbr_t>(item).timestamp.load() ==
                         std::numeric_limits<timestamp_t>::max()
                     ? 0
                     : 1);
    if (ret.size() != 0) {
      ret.set_mmap_array(&nbr_list_);
      ret.set_buffer(nullptr);
      ret.set_start_idx(i);
      ret.set_size(1);
    }
    return ret;
  }
  gbp::BufferObject get_edge(vid_t i) const { return nbr_list_.get(i); }

#endif
  size_t get_index_size_in_byte() const override { return 0; }
  size_t get_data_size_in_byte() const override {
    return nbr_list_.get_size_in_byte();
  }

  void ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc, timestamp_t ts,
                   MMapAllocator& alloc) override {
    EDATA_T value;
    arc >> value;
    put_edge(src, dst, value, ts, alloc);
  }

  void peek_ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc,
                        timestamp_t ts, MMapAllocator& alloc) override {
    EDATA_T value;
    arc.Peek<EDATA_T>(value);
    put_edge(src, dst, value, ts, alloc);
  }

  std::shared_ptr<MutableCsrConstEdgeIterBase> edge_iter(
      vid_t v) const override {
    return std::make_shared<TypedMutableCsrConstEdgeIter<EDATA_T>>(
        get_edges(v));
  }

  MutableCsrConstEdgeIterBase* edge_iter_raw(vid_t v) const override {
    return new TypedMutableCsrConstEdgeIter<EDATA_T>(get_edges(v));
  }

  std::shared_ptr<MutableCsrEdgeIterBase> edge_iter_mut(vid_t v) override {
    return std::make_shared<TypedMutableCsrEdgeIter<EDATA_T>>(get_edges_mut(v));
  }

 private:
  mmap_array<nbr_t> nbr_list_;
};

template <typename EDATA_T>
class EmptyCsr : public TypedMutableCsrBase<EDATA_T> {
  using slice_t = MutableNbrSlice<EDATA_T>;

 public:
  EmptyCsr() = default;
  ~EmptyCsr() = default;

  void batch_init(const std::string& name, const std::string& work_dir,
                  const std::vector<int>& degree) override {}

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) override {
    LOG(INFO) << "EmptyCsr";
  }

  void dump(const std::string& name,
            const std::string& new_spanshot_dir) override {}

  void resize(vid_t vnum) override {}

  size_t size() const override { return 0; }

  slice_t get_edges(vid_t i) const override { return slice_t::empty(); }

  void put_generic_edge(vid_t src, vid_t dst, const Any& data, timestamp_t ts,
                        MMapAllocator&) override {}

  void batch_put_edge(vid_t src, vid_t dst, const EDATA_T& data,
                      timestamp_t ts = 0) override {}

  void ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc, timestamp_t ts,
                   MMapAllocator&) override {
    EDATA_T value;
    arc >> value;
  }

  void peek_ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc,
                        const timestamp_t ts, MMapAllocator&) override {}

  std::shared_ptr<MutableCsrConstEdgeIterBase> edge_iter(
      vid_t v) const override {
    return std::make_shared<TypedMutableCsrConstEdgeIter<EDATA_T>>(
        MutableNbrSlice<EDATA_T>::empty());
  }
  MutableCsrConstEdgeIterBase* edge_iter_raw(vid_t v) const override {
    return new TypedMutableCsrConstEdgeIter<EDATA_T>(
        MutableNbrSlice<EDATA_T>::empty());
  }
  std::shared_ptr<MutableCsrEdgeIterBase> edge_iter_mut(vid_t v) override {
    return std::make_shared<TypedMutableCsrEdgeIter<EDATA_T>>(
        MutableNbrSliceMut<EDATA_T>::empty());
  }
  size_t get_index_size_in_byte() const override { return 0; }
  size_t get_data_size_in_byte() const override { return 0; }
};
}  // namespace gs
#endif  // GRAPHSCOPE_GRAPH_MUTABLE_CSR_H_
