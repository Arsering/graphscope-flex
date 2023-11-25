/**
 * Copyright 2022 AntGroup CO., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

#pragma once

#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include "flex/utils/property/types.h"
#include "glog/logging.h"

namespace gs {

namespace gbp {
#define LBMalloc(size) malloc(size)
#define LBFree(p) free(p)
#define LBRealloc(p, size) realloc((p), (size))
/**
 * Representation for a memory block. It can be used to store a const reference
 * to a memory block, or a memory block malloced, and thus owned by the
 * BufferObject object. A const reference is valid only when the memory block it
 * refers to is valid. It does not free the memory block when the BufferObject
 * object is destructed. A BufferObject owning a memory block will free its
 * memory when the BufferObject object is destructed.
 *
 * \note  The implementation uses small-object optimization. When the memory
 * block is smaller or equal to 64 bytes, it just uses stack memory.
 */
class BufferObjectImp {
  char* data_ = nullptr;
  size_t size_ = 0;
  bool need_delete_ = false;

  void Malloc(size_t s) {
    data_ = (char*) LBMalloc(s);
    // FMA_ASSERT(data_) << "Allocation falied, size = " << s;
    if (data_ == NULL)
      LOG(FATAL) << "Allocation failed!! (size = " << std::to_string(s);
    need_delete_ = true;
    size_ = s;
  }

 public:
  /** Construct an empty value */
  template <typename INNER_T>
  BufferObjectImp() {
    Malloc(sizeof(INNER_T));
  }

  BufferObjectImp() {
    data_ = nullptr;
    size_ = 0;
    need_delete_ = false;
  }

  /**
   * Construct a BufferObjectImp object with the specified size.
   *
   * 会使用 malloc() 函数分配内存
   *
   * \param   s   Size of the memory block to allocate.
   */
  explicit BufferObjectImp(size_t s) { Malloc(s); }

  // explicit BufferObjectImp(size_t s, uint8_t fill) {
  //   Malloc(s);
  //   memset(Data(), fill, s);
  // }

  BufferObjectImp(std::string_view& val) {
    data_ = const_cast<char*>(val.data());
    size_ = val.size();
    need_delete_ = false;
  }

  /**
   * Construct a const reference to the given memory block.
   *
   * \param   buf Pointer to the memory block.
   * \param   s   Size of the buffer.
   */
  explicit BufferObjectImp(const void* buf, size_t s) {
    data_ = (char*) (buf);
    size_ = s;
    need_delete_ = false;
  }

  /**
   * Set the BufferObjectImp to the const reference to the given memory block.
   *
   * \param   buf Pointer to the memory block.
   * \param   s   Size of the memory block.
   */
  void AssignConstRef(const char* buf, size_t s) {
    if (need_delete_)
      LBFree(data_);
    data_ = const_cast<char*>(buf);
    size_ = s;
    need_delete_ = false;
  }

  /**
   * Take ownership of the buf given.
   *
   * \param   buf Pointer to the memory block.
   * \param   s   Size of the memory block.
   */
  void TakeOwnershipFrom(void* buf, size_t s) {
    if (need_delete_)
      LBFree(data_);
    data_ = static_cast<char*>(buf);
    size_ = s;
    need_delete_ = true;
  }

  // /**
  //  * Constructs a const reference to the memory block given in the MDB_val
  //  * object.
  //  *
  //  * \param   val An MDB_val describing memory block and its size.
  //  */
  // explicit BufferObjectImp(const MDB_val &val)
  //     : data_(static_cast<char *>(val.mv_data)), size_(val.mv_size),
  //     need_delete_(false) {}

  /**
   * Construct a BufferObjectImp object by copying the content of the string.
   *
   * \param   buf The string to copy.
   */
  explicit BufferObjectImp(const std::string& buf) {
    Malloc(buf.size());
    memcpy(data_, buf.data(), buf.size());
  }

  explicit BufferObjectImp(const char* buf) {
    data_ = (char*) buf;
    size_ = strlen(buf);
    need_delete_ = false;
  }

  BufferObjectImp(const BufferObjectImp& rhs) {
    if (rhs.need_delete_) {
      Malloc(rhs.size_);
      memcpy(data_, rhs.data_, rhs.size_);
    } else {
      data_ = rhs.data_;
      size_ = rhs.size_;
      need_delete_ = false;
    }
  }

  BufferObjectImp(BufferObjectImp&& rhs) = delete;

  BufferObjectImp& operator=(const BufferObjectImp& rhs) {
    if (this == &rhs)
      return *this;
    if (rhs.need_delete_) {
      if (need_delete_) {
        // FMA_DBG_ASSERT(data_ != stack_);
        data_ = (char*) LBRealloc(data_, rhs.size_);
        size_ = rhs.size_;
      } else {
        Malloc(rhs.size_);
      }
      memcpy(data_, rhs.data_, rhs.size_);
    } else {
      data_ = rhs.data_;

      size_ = rhs.size_;
      need_delete_ = false;
    }
    return *this;
  }

  BufferObjectImp& operator=(BufferObjectImp&& rhs) {
    if (this == &rhs)
      return *this;
    if (need_delete_)
      LBFree(data_);
    need_delete_ = rhs.need_delete_;
    size_ = rhs.size_;

    data_ = rhs.data_;
    rhs.need_delete_ = false;
    rhs.size_ = 0;

    return *this;
  }

  void Clear() {
    if (need_delete_)
      LBFree(data_);
    data_ = nullptr;
    size_ = 0;
    need_delete_ = false;
  }

  void AssignConstRef(void* p, size_t s) {
    if (need_delete_)
      LBFree(data_);
    need_delete_ = false;
    data_ = (char*) p;
    size_ = s;
  }

  template <typename T>
  void AssignConstRef(const T& d) {
    AssignConstRef(&d, sizeof(T));
  }

  void AssignConstRef(const std::string& str) {
    AssignConstRef(str.data(), str.size());
  }

  /**
   * Equality operator. Compares by the byte content.
   *
   * @param rhs   The right hand side.
   *
   * @return  True if the parameters are considered equivalent.
   */
  bool operator==(const BufferObjectImp& rhs) const {
    return Size() == rhs.Size() && memcmp(Data(), rhs.Data(), Size()) == 0;
  }

  bool operator!=(const BufferObjectImp& rhs) const { return !(*this == rhs); }

#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wfree-nonheap-object"
#endif
  ~BufferObjectImp() {
    if (need_delete_) {
      // std::cout << "free space" << std::endl;
      LBFree(data_);
    }
    // std::cout << "~value" << std::endl;
  }
#ifdef __GNUC__
#pragma GCC diagnostic pop
#endif
  /**
   * @brief 新建一个 BufferObjectImp 对象，属性值与 rhs 一致
   *
   * @param rhs
   * @return BufferObjectImp
   */
  // static BufferObjectImp MakeCopy(const BufferObjectImp& rhs) {
  //   BufferObjectImp v;
  //   v.Copy(rhs);
  //   return v;
  // }

  // static BufferObjectImp MakeCopy(const MDB_val &v)
  // {
  //     BufferObjectImp rv(v.mv_size);
  //     memcpy(rv.Data(), v.mv_data, v.mv_size);
  //     return rv;
  // }

  // make a copy of current data
  // BufferObjectImp MakeCopy() const {
  //   BufferObjectImp v;
  //   v.Copy(*this);
  //   return v;
  // }

  template <typename T>
  void Copy(const T& d) {
    Resize(sizeof(T));
    memcpy(data_, &d, sizeof(T));
  }

  void Copy(const char* buf, size_t s) {
    Resize(s);
    memcpy(data_, buf, s);
  }

  void Copy(const std::string& s) {
    Resize(s.size());
    memcpy(data_, s.data(), s.size());
  }

  /**
   * @brief 用 rhs 的属性值覆盖当前 BufferObjectImp 对象的属性值
   *
   *
   * Make a copy of the memory referred to by rhs. The new memory block is owned
   * by *this.
   *
   * \param   rhs The right hand side.
   */
  void Copy(const BufferObjectImp& rhs) {
    Resize(rhs.Size());
    memcpy(data_, rhs.data_, rhs.Size());
  }

  // /**
  //  * Make a copy of the memory referred to by v. The new memory block is
  //  owned
  //  * by *this.
  //  *
  //  * \param   v   A MDB_val.
  //  */
  // void Copy(MDB_val v)
  // {
  //     Resize(v.mv_size);
  //     memcpy(data_, v.mv_data, v.mv_size);
  // }

  /**
   * Gets the pointer to memory block.
   *
   * \return  Memory block referred to by this
   */
  char* Data() const { return data_; }

  template <typename INNER_T>
  INNER_T& Obj() {
    if (size_ != sizeof(INNER_T)) {
      std::cerr << "size_!=sizeof(INNER_T)" << std::endl;
      exit(-1);
    }
    return *reinterpret_cast<INNER_T*>(data_);
  }

  /**
   * Gets the size of the memory block
   *
   * \return  A size_t.
   */
  size_t Size() const { return size_; }

  /**
   * If this BufferObjectImp empty?
   *
   * \return  True if it succeeds, false if it fails.
   */
  bool Empty() const { return size_ == 0; }

  // /**
  //  * Makes mdb value that refers to current memory block.
  //  *
  //  * \return  A MDB_val.
  //  */
  // MDB_val MakeMdbVal() const
  // {
  //     MDB_val v;
  //     v.mv_data = data_;
  //     v.mv_size = size_;
  //     return v;
  // }

  /**
   * Resizes the memory block. If *this is a const reference, a new memory block
   * is created so *this will own the memory.
   *
   * \param   s       Size of the new memory block.
   * \param   reserve (Optional) Size of the memory block to reserve. Although
   * this->Size() will be equal to s, the underlying memory block is at least
   * reserve bytes. This is to reduce further memory allocation if we need to
   * expand the memory block later.
   */
  void Resize(size_t s, size_t reserve = 0) {
    size_t msize = std::max<size_t>(reserve, s);
    if (need_delete_) {
      if (msize > size_) {
        // do realloc only if we are expanding
        data_ = (char*) LBRealloc(data_, msize);
        if (data_ == nullptr)
          std::cerr << "Allocation failed" << std::endl;
        // FMA_ASSERT(data_ != nullptr) << "Allocation failed";
      }
    } else {
      if (data_ != nullptr) {
        void* oldp = data_;
        size_t olds = size_;
        Malloc(msize);
        if (oldp != data_) {
          memcpy(data_, oldp, std::min<size_t>(olds, s));
        }
      } else {
        Malloc(msize);
      }
    }
    size_ = s;
  }

  /**
   * Converts this object to data of type T
   *
   * \tparam  T   Type of data to convert to
   *
   * \return  A object of type T.
   */
  template <typename T>
  T AsType() const {
    if (sizeof(T) != size_) {
      std::cerr << "Wrong type" << std::endl;
      exit(-1);
    }
    T d;
    memcpy(&d, data_, size_);
    return d;
  }

  /**
   * Converts this object to a string. The memory is copied as-is into the
   * string.
   *
   * \return  A std::string which has size()==this->Size() and
   * data()==this->Data()
   */
  std::string AsString() const { return AsType<std::string>(); }

  /**
   * Create a BufferObjectImp that is a const reference to the object t
   *
   * \tparam  T   Generic type parameter.
   * \param   t   A T to process.
   *
   * \return  A BufferObjectImp.
   */
  template <typename T, size_t S = sizeof(T)>
  static BufferObjectImp ConstRef(const T& t) {
    return BufferObjectImp(&t, S);
  }

  static BufferObjectImp ConstRef(const BufferObjectImp& v) {
    return BufferObjectImp(v.Data(), v.Size());
  }

  /**
   * Create a BufferObjectImp that is a const reference to s.c_str()
   *
   * \param   s   A std::string.
   *
   * \return  A BufferObjectImp.
   */
  static BufferObjectImp ConstRef(const std::string& s) {
    return BufferObjectImp(s.data(), s.size());
  }

  /**
   * Create a BufferObjectImp that is a const reference to a c-string
   *
   * \param   s   The c-string
   *
   * \return  A BufferObjectImp.
   */
  static BufferObjectImp ConstRef(const char* const& s) {
    return BufferObjectImp(s, std::strlen(s));
  }

  /**
   * Create a BufferObjectImp that is a const reference to a c-string
   *
   * \param   s   The c-string
   *
   * \return  A BufferObjectImp.
   */
  static BufferObjectImp ConstRef(char* const& s) {
    return BufferObjectImp(s, std::strlen(s));
  }

  /**
   * Create a BufferObjectImp that is a const reference to a string literal
   *
   * \param   s   The string literal
   *
   * \return  A BufferObjectImp.
   */
  template <size_t S>
  static BufferObjectImp ConstRef(const char (&s)[S]) {
    return BufferObjectImp(s, S - 1);
  }

  std::string DebugString(size_t line_width = 32) const {
    const char N2C[] = {'0', '1', '2', '3', '4', '5', '6', '7',
                        '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};
    const uint8_t* ptr = (const uint8_t*) Data();
    size_t s = Size();
    std::string ret;
    for (size_t i = 0; i < s; i++) {
      if (i % line_width == 0 && i != 0)
        ret.push_back('\n');
      uint8_t c = ptr[i];
      ret.push_back(N2C[c >> 4]);
      ret.push_back(N2C[c & 0xF]);
      if (i != s - 1)
        ret.push_back(' ');
    }
    return ret;
  }
};
// enum class PropertyType {
//   kInt32,
//   kDate,
//   kString,
//   kEmpty,
//   kInt64,
//   kDouble,
// };

enum class ObjectType {
  gbpAny,
  gbpClass,
  gbpEmpty,
};

class BufferObject {
 private:
  std::shared_ptr<BufferObjectImp> inner_;
  ObjectType type_;

 public:
  BufferObject() { inner_ = std::make_shared<BufferObjectImp>(); }
  BufferObject(size_t s) { inner_ = std::make_shared<BufferObjectImp>(s); }
  template <typename T>
  BufferObject() {
    inner_ = std::make_shared<BufferObjectImp>(sizeof(T));
  }
  static BufferObject Copy(const BufferObject& rhs) {
    BufferObject ret(rhs.Size());
    memcpy(ret.Data(), rhs.Data(), rhs.Size());
    return ret;
  }

  BufferObject(const Any& value) {
    type_ = ObjectType::gbpAny;
    if (value.type == PropertyType::kInt32) {
      inner_ = std::make_shared<BufferObjectImp>(sizeof(int32_t));
      type_ = ObjectType::gbpAny;
      memcpy(inner_->Data(), &value.value.i, sizeof(int32_t));
    } else if (value.type == PropertyType::kInt64) {
      inner_ = std::make_shared<BufferObjectImp>(sizeof(int64_t));
      memcpy(inner_->Data(), &value.value.l, sizeof(int64_t));
    } else if (value.type == PropertyType::kString) {
      inner_ = std::make_shared<BufferObjectImp>(sizeof(std::string_view));
      memcpy(inner_->Data(), &value.value.s, sizeof(std::string_view));
      //      return value.s.to_string();
    } else if (value.type == PropertyType::kDate) {
      inner_ = std::make_shared<BufferObjectImp>(sizeof(Date));
      memcpy(inner_->Data(), &value.value.d, sizeof(Date));
    } else if (value.type == PropertyType::kEmpty) {
      inner_ = std::make_shared<BufferObjectImp>();
    } else if (value.type == PropertyType::kDouble) {
      inner_ = std::make_shared<BufferObjectImp>(sizeof(double));
      memcpy(inner_->Data(), &value.value.db, sizeof(double));
    } else {
      LOG(FATAL) << "Unexpected property type: "
                 << static_cast<int>(value.type);
    }
  }

  BufferObject(const BufferObject& rhs) : inner_(rhs.inner_) {}

  const char* Data() const { return inner_->Data(); }
  char* Data() { return inner_->Data(); }
  size_t Size() const { return inner_->Size(); }

  // template <typename T>
  // T& Obj() {
  //   return inner_->Obj<T>();
  // }
  template <typename T>
  const T& Obj() const {
    return inner_->Obj<T>();
  }

  std::string to_string() const {
    if (type_ != ObjectType::gbpAny)
      LOG(FATAL) << "Can't convert current type to std::string!!";
    auto value = reinterpret_cast<const Any*>(Data());
    return value->to_string();
  }
};

template <typename T>
T& Decode(BufferObject& obj) {
  if (sizeof(T) != obj.Size()) {
    LOG(INFO) << "sizeof T = " << sizeof(T);
    LOG(INFO) << "sizeof obj = " << obj.Size();
    LOG(FATAL) << "Decode size mismatch!!!";
  }

  return *reinterpret_cast<T*>(obj.Data());
}

template <typename T>
const T& Decode(const BufferObject& obj) {
  if (sizeof(T) != obj.Size()) {
    LOG(INFO) << "sizeof T = " << sizeof(T);
    LOG(INFO) << "sizeof obj = " << obj.Size();
    LOG(FATAL) << "Decode size mismatch!!!";
  }

  return *reinterpret_cast<const T*>(obj.Data());
}

}  // namespace gbp
}  // namespace gs
