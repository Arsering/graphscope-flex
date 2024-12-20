#pragma once

#include <cstdint>
#include <string>

namespace gs {

template <typename VID>
class BaseIndexer {
 public:
  virtual ~BaseIndexer() = default;

  // 基类定义的公共接口
  virtual bool get_index(int64_t oid, VID& lid) const = 0;
  virtual int64_t get_key(const VID& lid) const = 0;
  virtual size_t size() const = 0;

  // 序列化相关
  virtual void dump(const std::string& name, const std::string& snapshot_dir) {
    // 空实现
  }
  virtual void open(const std::string& name, const std::string& snapshot_dir,
                    const std::string& work_dir) = 0;
};

}  // namespace gs