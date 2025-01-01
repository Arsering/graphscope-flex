#pragma once

#include <string>
#include <vector>

#include "flex/graphscope_bufferpool/include/mmap_array.h"
#include "flex/utils/mmap_array.h"

namespace gs {
namespace cgraph {
class ColumnHandle {
 public:
  ColumnHandle(const gbp::mmap_array& property_buffer,
               const size_t& row_capacity, const size_t& offset,
               const size_t& columnLength)
      : property_buffer_(property_buffer),
        row_capacity_(row_capacity),
        offset_(offset),
        columnLength_(columnLength) {}
  ~ColumnHandle() = default;
  void setColumn(size_t rowId, std::string_view newValue) { assert(false); }
  FORCE_INLINE gbp::BufferBlock getColumn(size_t rowId) const {
    return property_buffer_.get_partial(rowId, offset_, columnLength_);
  }

 private:
  const gbp::mmap_array& property_buffer_;
  const size_t& row_capacity_;  // 最大的容纳量
  const size_t& offset_;        // 当前column的偏移量
  const size_t& columnLength_;  // 单个column的长度
};

class FixedLengthColumnFamily {
 private:
  gbp::mmap_array property_buffer_;
  size_t row_capacity_;
  std::vector<size_t> offsets_;  // 记录column family中各个column的偏移量
  std::vector<size_t> columnLengths_;

 public:
  FixedLengthColumnFamily() = default;
  ~FixedLengthColumnFamily() = default;

  void Open(const std::vector<size_t>& ColumnLengths,
            const std::string& filename) {
    columnLengths_ = ColumnLengths;
    // 计算column family中各个column的偏移量
    size_t offset = 0;
    for (size_t length : ColumnLengths) {
      offsets_.push_back(offset);
      offset += length;
    }
    offsets_.push_back(offset);
    property_buffer_.open(filename, false, offset);
    row_capacity_ = property_buffer_.size();
  }

  // 设置单个column的值 (注意修改是非atomic的)
  void setColumn(size_t rowId, size_t columnId, std::string_view newValue) {
#if ASSERT_ENABLE
    assert(rowId < row_capacity_);
    assert(columnId < offsets_.size());
    assert(newValue.size() == columnLengths_[columnId]);
#endif
    // 更新value
    property_buffer_.set_partial(rowId, offsets_[columnId],
                                 {newValue.data(), newValue.size()});
  }

  // 设置整个column family的值 (注意修改是非atomic的)
  void setColumnFamily(size_t rowId, size_t columnId,
                       std::string_view newValue) {
#if ASSERT_ENABLE
    assert(rowId < row_capacity_);
    assert(columnId < offsets_.size());
    assert(newValue.size() == columnLengths_[columnId]);
#endif
    // 更新value
    property_buffer_.set(rowId * offsets_.back(), newValue.data(),
                         newValue.size());
  }

  // 获取单个column的值
  // 若要实现atomic，则需要使用gbp::BufferBlock
  gbp::BufferBlock getColumn(size_t rowId, size_t columnId) const {
#if ASSERT_ENABLE
    assert(rowId < row_capacity_);
    assert(columnId < offsets_.size());
#endif
    return property_buffer_.get_partial(rowId, offsets_[columnId],
                                        columnLengths_[columnId]);
  }

  ColumnHandle getColumnHandle(size_t columnId) const {
    return ColumnHandle(property_buffer_, row_capacity_, offsets_[columnId],
                        columnLengths_[columnId]);
  }

  // 获取column的长度
  size_t getPropertyLength(size_t columnId) const {
    return columnLengths_[columnId];
  }
  // 获取column的长度
  size_t getSizeInByte() const { return property_buffer_.get_size_in_byte(); }
  // size的单位为byte
  void resize(size_t size) {
    property_buffer_.resize(size);
    row_capacity_ = size;
  }
};

}  // namespace cgraph
}  // namespace gs