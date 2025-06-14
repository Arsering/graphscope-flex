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

#ifndef GRAPHSCOPE_PROPERTY_COLUMN_H_
#define GRAPHSCOPE_PROPERTY_COLUMN_H_

#include <string>
#include <string_view>

#include "flex/utils/mmap_array.h"
#include "flex/utils/property/types.h"
#include "grape/serialization/out_archive.h"

namespace gs {
class ColumnBase {
 public:
  virtual ~ColumnBase() {}

  virtual void open(const std::string& name, const std::string& snapshot_dir,
                    const std::string& work_dir) = 0;

  virtual void touch(const std::string& filename) = 0;

  virtual void dump(const std::string& filename) = 0;

  virtual size_t size() const = 0;

  virtual void resize(size_t size) = 0;

  virtual PropertyType type() const = 0;

  virtual void set_any(size_t index, const Any& value) = 0;
#if OV
  virtual Any get(size_t index) const = 0;
#else
  virtual gbp::BufferBlock get(size_t index) const = 0;
  virtual void set(size_t index, const gbp::BufferBlock& value) = 0;
  virtual gbp::batch_request_type get_batch(size_t idx, size_t offset,
                                            size_t len = 1) const = 0;
  virtual gbp::batch_request_type get_stringview_batch(size_t idx) const = 0;
#endif
  virtual size_t get_size_in_byte() const = 0;
  virtual void ingest(uint32_t index, grape::OutArchive& arc) = 0;

  virtual StorageStrategy storage_strategy() const = 0;
};

#if !OV
class ColumnBaseAsync {
 public:
  virtual ~ColumnBaseAsync() {}

  virtual void open(const std::string& name, const std::string& snapshot_dir,
                    const std::string& work_dir) = 0;

  virtual void touch(const std::string& filename) = 0;

  virtual void dump(const std::string& filename) = 0;

  virtual size_t size() const = 0;

  virtual void resize(size_t size) = 0;

  virtual PropertyType type() const = 0;

  virtual void set_any(size_t index, const Any& value) = 0;

  virtual std::future<gbp::BufferBlock> get_async(size_t index) const = 0;
  virtual void set(size_t index, const gbp::BufferBlock& value) = 0;

  virtual size_t get_size_in_byte() const = 0;
  virtual void ingest(uint32_t index, grape::OutArchive& arc) = 0;

  virtual StorageStrategy storage_strategy() const = 0;
};

#endif

template <typename T>
class TypedColumn : public ColumnBase {
 public:
  TypedColumn(StorageStrategy strategy) : strategy_(strategy) {}
  ~TypedColumn() {}

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) {
    std::string basic_path = snapshot_dir + "/" + name;
    if (std::filesystem::exists(basic_path)) {
      basic_buffer_.open(basic_path, true);
      basic_size_ = basic_buffer_.size();
    } else {
      basic_size_ = 0;
    }

    extra_buffer_.open(work_dir + "/" + name, false);
    extra_size_ = extra_buffer_.size();
  }

  void touch(const std::string& filename) override {
    mmap_array<T> tmp;
    tmp.open(filename, false);
    tmp.resize(basic_size_ + extra_size_);
#if OV
    for (size_t k = 0; k < basic_size_; ++k) {
      tmp.set(k, basic_buffer_.get(k));
    }
    for (size_t k = 0; k < extra_size_; ++k) {
      tmp.set(k + basic_size_, extra_buffer_.get(k));
    }
#else
    assert(false);
    // auto basic_buffer_items = basic_buffer_.get(0, basic_size_);
    // tmp.set(0, basic_buffer_items);
    // basic_buffer_items.free();
    // auto extra_buffer_items = extra_buffer_.get(0, extra_size_);
    // tmp.set(basic_size_, extra_buffer_items);
#endif
    basic_size_ = 0;
    basic_buffer_.reset();
    extra_size_ = tmp.size();
    extra_buffer_.swap(tmp);
  }

#if OV
  void dump(const std::string& filename) override {
    if (basic_size_ != 0 && extra_size_ == 0) {
      basic_buffer_.dump(filename);
    } else if (basic_size_ == 0 && extra_size_ != 0) {
      extra_buffer_.dump(filename);
    } else {
      mmap_array<T> tmp;
      tmp.open(filename, false);
      tmp.resize(basic_size_ + extra_size_);
      for (size_t k = 0; k < basic_size_; ++k) {
        tmp.set(k, basic_buffer_.get(k));
      }
      for (size_t k = 0; k < extra_size_; ++k) {
        tmp.set(k + basic_size_, extra_buffer_.get(k));
      }
    }
  }
#else
  void dump(const std::string& filename) override {
    if (basic_size_ != 0 && extra_size_ == 0) {
      basic_buffer_.dump(filename);
    } else if (basic_size_ == 0 && extra_size_ != 0) {
      extra_buffer_.dump(filename);
    } else {
      mmap_array<T> tmp;
      tmp.open(filename, false);
      tmp.resize(basic_size_ + extra_size_);

      auto basic_buffer_items_old = basic_buffer_.get(0, basic_size_);
      auto basic_buffer_items_new = tmp.get(0, basic_size_);
      for (size_t i = 0; i < basic_size_; i++)
        gbp::BufferBlock::UpdateContent<T>(
            [&](T& item) {
              memcpy(&item,
                     &gbp::BufferBlock::Ref<T>(basic_buffer_items_old, i),
                     sizeof(T));
            },
            basic_buffer_items_new, i);

      auto extra_buffer_items_old = extra_buffer_.get(0, extra_size_);
      auto extra_buffer_items_new = tmp.get(basic_size_, extra_size_);
      for (size_t i = 0; i < extra_size_; i++)
        gbp::BufferBlock::UpdateContent<T>(
            [&](T& item) {
              memcpy(&item,
                     &gbp::BufferBlock::Ref<T>(extra_buffer_items_old, i),
                     sizeof(T));
            },
            extra_buffer_items_new, i);
    }
  }
#endif
  size_t size() const override { return basic_size_ + extra_size_; }

  void resize(size_t size) override {
    if (size < basic_buffer_.size()) {
      basic_size_ = size;
      extra_size_ = 0;
    } else {
      basic_size_ = basic_buffer_.size();
      extra_size_ = size - basic_size_;
      extra_buffer_.resize(extra_size_);
    }
  }

  PropertyType type() const override { return AnyConverter<T>::type; }

#if OV
  void set_value(size_t index, const T& val) {
    assert(index >= basic_size_ && index < basic_size_ + extra_size_);
    extra_buffer_.set(index - basic_size_, val);
  }

  void set_any(size_t index, const Any& value) override {
    set_value(index, AnyConverter<T>::from_any(value));
  }

  T get_view(size_t index) const {
    return index < basic_size_ ? basic_buffer_.get(index)
                               : extra_buffer_.get(index - basic_size_);
  }

  Any get(size_t index) const override {
    return AnyConverter<T>::to_any(get_view(index));
  }
#else
  void set_value(size_t index, const T& val) {
#if ASSERT_ENABLE
    assert(index >= basic_size_ && index < basic_size_ + extra_size_);
#endif
    // 为了防止一个obj跨两个页
    if constexpr (gbp::PAGE_SIZE_FILE / sizeof(T) == 0)
      extra_buffer_.set(index - basic_size_, val);
    else {
      auto item_t = extra_buffer_.get(index - basic_size_);
      gbp::BufferBlock::UpdateContent<T>([&](T& item) { item = val; }, item_t);
    }
  }

  void set_any(size_t index, const Any& value) override {
    set_value(index, AnyConverter<T>::from_any(value));
  }

  void set(size_t index, const gbp::BufferBlock& value) override {
    auto val = gbp::BufferBlock::Ref<T>(value);
    set_value(index, val);
  }

  FORCE_INLINE gbp::BufferBlock get_inner(size_t idx) const {
    return idx < basic_size_ ? basic_buffer_.get(idx)
                             : extra_buffer_.get(idx - basic_size_);
  }
  gbp::BufferBlock get(size_t idx) const override { return get_inner(idx); }

  gbp::batch_request_type get_batch(size_t idx, size_t offset = 0,
                                    size_t len = 1) const override {
#if ASSERT_ENABLE
    assert(idx < basic_size_ + extra_size_);
    assert(len == 1);
#endif
    return idx < basic_size_ ? basic_buffer_.get_batch(idx)
                             : extra_buffer_.get_batch(idx - basic_size_);
  }
  gbp::batch_request_type get_stringview_batch(size_t idx) const override {
    assert(false);
    return gbp::batch_request_type();
  }
#endif
  size_t get_size_in_byte() const override {
    return basic_buffer_.get_size_in_byte() + extra_buffer_.get_size_in_byte();
  }
  void ingest(uint32_t index, grape::OutArchive& arc) override {
    T val;
    arc >> val;
    set_value(index, val);
  }

  StorageStrategy storage_strategy() const override { return strategy_; }

  const mmap_array<T>& basic_buffer() const { return basic_buffer_; }
  size_t basic_buffer_size() const { return basic_size_; }
  const mmap_array<T>& extra_buffer() const { return extra_buffer_; }
  size_t extra_buffer_size() const { return extra_size_; }

 private:
  mmap_array<T> basic_buffer_;
  size_t basic_size_;
  mmap_array<T> extra_buffer_;
  size_t extra_size_;
  StorageStrategy strategy_;
};

using IntColumn = TypedColumn<int>;
using LongColumn = TypedColumn<int64_t>;
using DateColumn = TypedColumn<Date>;

class StringColumn : public ColumnBase
#if !OV
    ,
                     public ColumnBaseAsync
#endif
{
 public:
  StringColumn(StorageStrategy strategy, size_t width = 1024) : width_(width) {}
  ~StringColumn() {}

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) {
    std::string basic_path = snapshot_dir + "/" + name;
    if (std::filesystem::exists(basic_path + ".items")) {
      basic_buffer_.open(basic_path, true);
      basic_size_ = basic_buffer_.size();
    } else {
      basic_size_ = 0;
    }

    extra_buffer_.open(work_dir + "/" + name, false);
    extra_size_ = extra_buffer_.size();
    pos_.store(extra_buffer_.data_size());
  }
#if OV
  void touch(const std::string& filename) override {
    mmap_array<std::string_view> tmp;
    tmp.open(filename, false);
    tmp.resize(basic_size_ + extra_size_, (basic_size_ + extra_size_) * width_);
    size_t offset = 0;
    for (size_t k = 0; k < basic_size_; ++k) {
      std::string_view val = basic_buffer_.get(k);
      tmp.set(k, offset, val);
      offset += val.size();
    }
    for (size_t k = 0; k < extra_size_; ++k) {
      std::string_view val = extra_buffer_.get(k);
      tmp.set(k + basic_size_, offset, val);
      offset += val.size();
    }

    basic_size_ = 0;
    basic_buffer_.reset();
    extra_size_ = tmp.size();
    extra_buffer_.swap(tmp);

    pos_.store(offset);
  }
#else

  void touch(const std::string& filename) override {
    mmap_array<std::string_view> tmp;
    tmp.open(filename, false);
    tmp.resize(basic_size_ + extra_size_, (basic_size_ + extra_size_) * width_);
    size_t offset = 0;
    for (size_t k = 0; k < basic_size_; ++k) {
      auto item = basic_buffer_.get(k);
      std::string_view val = {&item.Obj<char>(), item.Size()};
      tmp.set(k, offset, val);
      offset += val.size();
    }
    for (size_t k = 0; k < extra_size_; ++k) {
      auto item = extra_buffer_.get(k);
      std::string_view val = {&item.Obj<char>(), item.Size()};
      tmp.set(k + basic_size_, offset, val);
      offset += val.size();
    }

    basic_size_ = 0;
    basic_buffer_.reset();
    extra_size_ = tmp.size();
    extra_buffer_.swap(tmp);

    pos_.store(offset);
  }
#endif

  void dump(const std::string& filename) override {
    if (basic_size_ != 0 && extra_size_ == 0) {
      basic_buffer_.dump(filename);
    } else if (basic_size_ == 0 && extra_size_ != 0) {
      extra_buffer_.resize(extra_size_, pos_.load());
      extra_buffer_.dump(filename);
    } else {
      mmap_array<std::string_view> tmp;
      tmp.open(filename, false);
      tmp.resize(basic_size_ + extra_size_,
                 (basic_size_ + extra_size_) * width_);
      size_t offset = 0;
#if OV
      for (size_t k = 0; k < basic_size_; ++k) {
        std::string_view val = basic_buffer_.get(k);
        tmp.set(k, offset, val);
        offset += val.size();
      }
      for (size_t k = 0; k < extra_size_; ++k) {
        std::string_view val = extra_buffer_.get(k);
        tmp.set(k + basic_size_, offset, extra_buffer_.get(k));
        offset += val.size();
      }
#else
      // for (size_t k = 0; k < basic_size_; ++k) {
      //   auto item = basic_buffer_.get(k);
      //   std::string_view val = {item.Data(), item.Size()};
      //   tmp.set(k, offset, val);
      //   offset += val.size();
      // }
      // for (size_t k = 0; k < extra_size_; ++k) {
      //   auto item = extra_buffer_.get(k);
      //   std::string_view val = {item.Data(), item.Size()};
      //   tmp.set(k + basic_size_, offset, val);
      //   offset += val.size();
      // }
      assert(false);
#endif
      tmp.resize(basic_size_ + extra_size_, offset);
    }
  }

  size_t size() const override { return basic_size_ + extra_size_; }

  void resize(size_t size) override {
    if (size < basic_buffer_.size()) {
      basic_size_ = size;
      extra_size_ = 0;
    } else {
      basic_size_ = basic_buffer_.size();
      extra_size_ = size - basic_size_;
      extra_buffer_.resize(extra_size_, extra_size_ * width_);
    }
  }

  PropertyType type() const override {
    return AnyConverter<std::string_view>::type;
  }

  void set_value(size_t idx, const std::string_view& val) {
#if ASSERT_ENABLE
    assert(idx >= basic_size_ && idx < basic_size_ + extra_size_);
#endif
    size_t offset = pos_.fetch_add(val.size());
    extra_buffer_.set(idx - basic_size_, offset, val);
  }

  void set_any(size_t idx, const Any& value) override {
    set_value(idx, AnyConverter<std::string_view>::from_any(value));
  }

#if OV
  std::string_view get_view(size_t idx) const {
    return idx < basic_size_ ? basic_buffer_.get(idx)
                             : extra_buffer_.get(idx - basic_size_);
  }

  Any get(size_t idx) const override {
    return AnyConverter<std::string_view>::to_any(get_view(idx));
  }
#else
  gbp::BufferBlock get_inner(size_t idx) const {
    return idx < basic_size_ ? basic_buffer_.get(idx)
                             : extra_buffer_.get(idx - basic_size_);
  }
  gbp::BufferBlock get(size_t idx) const override { return get_inner(idx); }
  gbp::batch_request_type get_batch(size_t idx, size_t offset,
                                    size_t len) const override {
    return idx < basic_size_ ? basic_buffer_.get_string_batch(offset, len)
                             : extra_buffer_.get_string_batch(offset, len);
  }
  gbp::batch_request_type get_stringview_batch(size_t idx) const override {
    return idx < basic_size_
               ? basic_buffer_.get_stringview_batch(idx)
               : extra_buffer_.get_stringview_batch(idx - basic_size_);
  }

  std::future<gbp::BufferBlock> get_inner_async(size_t idx) const {
    return idx < basic_size_ ? basic_buffer_.get_async(idx)
                             : extra_buffer_.get_async(idx - basic_size_);
  }
  std::future<gbp::BufferBlock> get_async(size_t idx) const override {
    return get_inner_async(idx);
  }

  // TODO: 优化掉不必要的copy
  void set(size_t idx, const gbp::BufferBlock& value) override {
    std::string sv(value.Size(), 'a');
    value.Copy(sv.data(), value.Size());
    set_value(idx, sv);
    assert(false);
  }

#endif
  size_t get_size_in_byte() const override {
    return basic_buffer_.get_size_in_byte() + extra_buffer_.get_size_in_byte();
  }

  void ingest(uint32_t index, grape::OutArchive& arc) override {
    std::string_view val;
    arc >> val;
    set_value(index, val);
  }

  StorageStrategy storage_strategy() const override { return strategy_; }

  const mmap_array<std::string_view>& buffer() const { return basic_buffer_; }

 private:
  mmap_array<std::string_view> basic_buffer_;
  size_t basic_size_;
  mmap_array<std::string_view> extra_buffer_;
  size_t extra_size_;
  std::atomic<size_t> pos_;
  StorageStrategy strategy_;
  size_t width_;
};  // namespace gs

std::shared_ptr<ColumnBase> CreateColumn(
    PropertyType type, StorageStrategy strategy = StorageStrategy::kMem);

/// Create RefColumn for ease of usage for hqps
class RefColumnBase {
 public:
  virtual ~RefColumnBase() {}
};

// Different from TypedColumn, RefColumn is a wrapper of mmap_array
template <typename T>
class TypedRefColumn : public RefColumnBase {
 public:
  using value_type = T;

  TypedRefColumn(const mmap_array<T>& buffer, StorageStrategy strategy)
      : basic_buffer(buffer),
        basic_size(0),
        extra_buffer(buffer),
        extra_size(buffer.size()),
        strategy_(strategy) {}
  TypedRefColumn(const TypedColumn<T>& column)
      : basic_buffer(column.basic_buffer()),
        basic_size(column.basic_buffer_size()),
        extra_buffer(column.extra_buffer()),
        extra_size(column.extra_buffer_size()),
        strategy_(column.storage_strategy()) {}
  ~TypedRefColumn() {}

  inline T get_view(size_t index) const {
    return index < basic_size ? basic_buffer.get(index)
                              : extra_buffer.get(index - basic_size);
  }

 private:
  const mmap_array<T>& basic_buffer;
  size_t basic_size;
  const mmap_array<T>& extra_buffer;
  size_t extra_size;

  StorageStrategy strategy_;
};

}  // namespace gs

#endif  // GRAPHSCOPE_PROPERTY_COLUMN_H_
