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

#include "flex/utils/property/column.h"
#include "flex/utils/property/types.h"

#include "grape/serialization/out_archive.h"

namespace gs {

template <typename T>
class TypedEmptyColumn : public ColumnBase {
 public:
  TypedEmptyColumn() {}
  ~TypedEmptyColumn() {}

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) override {}
  void touch(const std::string& filename) override {}
  void dump(const std::string& filename) override {}
  size_t size() const override { return 0; }
  void resize(size_t size) override {}

  PropertyType type() const override { return AnyConverter<T>::type; }

  void set_value(size_t index, const T& val) {}
#if OV
  void set_any(size_t index, const Any& value) override {}

  T get_view(size_t index) const { T{}; }

  Any get(size_t index) const override { return Any(); }
#else
  void set_any(size_t index, const Any& value) override {}
  void set(size_t index, const gbp::BufferBlock& value) {}

  gbp::BufferBlock get(size_t index) const override {
    return gbp::BufferBlock();
  }
  gbp::batch_request_type get_batch(size_t idx, size_t offset,
                                    size_t len) const override {
    assert(false);
    return gbp::batch_request_type();
  }
  gbp::batch_request_type get_stringview_batch(size_t idx) const override {
    assert(false);
    return gbp::batch_request_type();
  }
#endif
  size_t get_size_in_byte() const override { return 0; }

  void ingest(uint32_t index, grape::OutArchive& arc) override {
    T val;
    arc >> val;
  }

  StorageStrategy storage_strategy() const override {
    return StorageStrategy::kNone;
  }
};

using IntEmptyColumn = TypedEmptyColumn<int>;
using LongEmptyColumn = TypedEmptyColumn<int64_t>;
using DateEmptyColumn = TypedEmptyColumn<Date>;

std::shared_ptr<ColumnBase> CreateColumn(PropertyType type,
                                         StorageStrategy strategy) {
  if (strategy == StorageStrategy::kNone) {
    if (type == PropertyType::kInt32) {
      return std::make_shared<IntEmptyColumn>();
    } else if (type == PropertyType::kInt64) {
      return std::make_shared<LongEmptyColumn>();
    } else if (type == PropertyType::kDate) {
      return std::make_shared<DateEmptyColumn>();
    } else if (type == PropertyType::kString) {
      return std::make_shared<TypedEmptyColumn<std::string_view>>();
    } else {
      LOG(FATAL) << "unexpected type to create column, "
                 << static_cast<int>(type);
      return nullptr;
    }
  } else {
    if (type == PropertyType::kInt32) {
      return std::make_shared<IntColumn>(strategy);
    } else if (type == PropertyType::kInt64) {
      return std::make_shared<LongColumn>(strategy);
    } else if (type == PropertyType::kDate) {
      return std::make_shared<DateColumn>(strategy);
    } else if (type == PropertyType::kString) {
      return std::make_shared<StringColumn>(strategy);
    } else {
      LOG(FATAL) << "unexpected type to create column, "
                 << static_cast<int>(type);
      return nullptr;
    }
  }
}

}  // namespace gs
