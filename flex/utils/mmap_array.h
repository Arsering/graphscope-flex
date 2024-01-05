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

#ifndef GRAPHSCOPE_UTILS_MMAP_ARRAY_H_
#define GRAPHSCOPE_UTILS_MMAP_ARRAY_H_

#include <assert.h>
#include <fcntl.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include "grape/util.h"

#include <atomic>
#include <filesystem>
#include <string>
#include <string_view>

#include "flex/graphscope_bufferpool/include/buffer_pool_manager.h"
#include "flex/utils/buffer_obj.h"
#include "glog/logging.h"

namespace gs {
#define s_size_t signed long int

inline void copy_file(const std::string& src, const std::string& dst) {
  if (!std::filesystem::exists(src)) {
    LOG(ERROR) << "file not exists: " << src;
    return;
  }
  size_t len = std::filesystem::file_size(src);
  int src_fd = ::open(src.c_str(), O_RDONLY, 0777);
  int dst_fd = ::open(dst.c_str(), O_WRONLY | O_CREAT, 0777);

  ssize_t ret;
  do {
    ret = copy_file_range(src_fd, NULL, dst_fd, NULL, len, 0);
    if (ret == -1) {
      perror("copy_file_range");
      return;
    }
    len -= ret;
  } while (len > 0 && ret > 0);
  close(src_fd);
  close(dst_fd);
}

template <typename T>
class mmap_array {
 public:
#if OV
  mmap_array()
      : filename_(""), fd_(-1), data_(NULL), size_(0), read_only_(true) {}
#else
  mmap_array() : filename_(""), size_(0), read_only_(true), fd_gbp_(-1) {
    buffer_pool_manager_ = &gbp::BufferPoolManager::GetGlobalInstance();
  }
#endif
  mmap_array(mmap_array&& rhs) : mmap_array() { swap(rhs); }

  ~mmap_array() {}
#if OV
  void reset() {
    filename_ = "";
    if (data_ != NULL) {
      munmap(data_, size_);
      data_ = NULL;
    }
    if (fd_ != -1) {
      close(fd_);
      fd_ = -1;
    }
    read_only_ = true;
  }
#else
  void reset() {
    filename_ = "";

    if (fd_gbp_ != -1) {
      buffer_pool_manager_->CloseFile(fd_gbp_);
      fd_gbp_ = -1;
    }
    read_only_ = true;
  }
#endif

#if OV
  void open(const std::string& filename, bool read_only) {
    reset();
    filename_ = filename;
    read_only_ = read_only;
    if (read_only) {
      if (!std::filesystem::exists(filename)) {
        LOG(ERROR) << "file not exists: " << filename;
        fd_ = 1;
        size_ = 0;
        data_ = NULL;
      } else {
        fd_ = ::open(filename.c_str(), O_RDONLY, 0777);
        size_t file_size = std::filesystem::file_size(filename);
        size_ = file_size / sizeof(T);
        if (size_ == 0) {
          data_ = NULL;
        } else {
          data_ = reinterpret_cast<T*>(
              mmap(NULL, size_ * sizeof(T), PROT_READ, MAP_PRIVATE, fd_, 0));
          Warmup((char*) data_, size_ * sizeof(T));
          madvise(data_, size_ * sizeof(T),
                  MMAP_ADVICE_l);  // Turn off readahead
          assert(data_ != MAP_FAILED);
        }
      }
    } else {
      fd_ = ::open(filename.c_str(), O_RDWR | O_CREAT, 0777);
      size_t file_size = std::filesystem::file_size(filename);
      size_ = file_size / sizeof(T);
      if (size_ == 0) {
        data_ = NULL;
      } else {
        data_ = reinterpret_cast<T*>(mmap(NULL, size_ * sizeof(T),
                                          PROT_READ | PROT_WRITE, MAP_SHARED,
                                          fd_, 0));
        Warmup((char*) data_, size_ * sizeof(T));

        madvise(data_, size_ * sizeof(T),
                MMAP_ADVICE_l);  // Turn off readahead

        assert(data_ != MAP_FAILED);
      }
    }
  }
#else
  void open(const std::string& filename, bool read_only) {
    reset();
    filename_ = filename;
    read_only_ = read_only;
    if (read_only) {
      if (!std::filesystem::exists(filename)) {
        LOG(ERROR) << "file not exists: " << filename;
        fd_gbp_ = -1;
        size_ = 0;
      } else {
        // fd_ = ::open(filename.c_str(), O_RDONLY | O_DIRECT);
        fd_gbp_ = buffer_pool_manager_->OpenFile(filename, O_RDONLY | O_DIRECT);
        size_t file_size = std::filesystem::file_size(filename);
        size_ = file_size / sizeof(T);
        // fd_inner_ = buffer_pool_manager_->RegisterFile(fd_);
      }
    } else {
      // fd_ = ::open(filename.c_str(), O_RDWR | O_CREAT | O_DIRECT);
      fd_gbp_ =
          buffer_pool_manager_->OpenFile(filename, O_RDWR | O_CREAT | O_DIRECT);
      size_t file_size = std::filesystem::file_size(filename);
      size_ = file_size / sizeof(T);
      // fd_inner_ = buffer_pool_manager_->RegisterFile(fd_);
    }
  }
#endif

#if OV
  void dump(const std::string& filename) {
    assert(!filename_.empty());
    assert(std::filesystem::exists(filename_));
    std::string old_filename = filename_;
    reset();
    if (read_only_) {
      std::filesystem::create_hard_link(old_filename, filename);
    } else {
      std::filesystem::rename(old_filename, filename);
    }
  }
#else
  void dump(const std::string& filename) {
    assert(!filename_.empty());
    assert(std::filesystem::exists(filename_));
    std::string old_filename = filename_;
    reset();
    if (read_only_) {
      std::filesystem::create_hard_link(old_filename, filename);
    } else {
      std::filesystem::rename(old_filename, filename);
    }
  }
#endif

#if OV
  void resize(size_t size) {
    assert(fd_ != -1);

    if (size == size_) {
      return;
    }

    if (read_only_) {
      if (size < size_) {
        munmap(data_, size_ * sizeof(T));
        size_ = size;
        data_ = reinterpret_cast<T*>(
            mmap(NULL, size_ * sizeof(T), PROT_READ, MAP_PRIVATE, fd_, 0));
        Warmup((char*) data_, size_ * sizeof(T));

        madvise(data_, size_ * sizeof(T),
                MMAP_ADVICE_l);  // Turn off readahead

      } else if (size * sizeof(T) < std::filesystem::file_size(filename_)) {
        munmap(data_, size_ * sizeof(T));
        size_ = size;
        data_ = reinterpret_cast<T*>(
            mmap(NULL, size_ * sizeof(T), PROT_READ, MAP_PRIVATE, fd_, 0));
        Warmup((char*) data_, size_ * sizeof(T));

        madvise(data_, size_ * sizeof(T),
                MMAP_ADVICE_l);  // Turn off readahead
      } else {
        LOG(FATAL)
            << "cannot resize read-only mmap_array to larger size than file";
      }
    } else {
      if (data_ != NULL) {
        munmap(data_, size_ * sizeof(T));
      }
      auto ret = ::ftruncate(fd_, size * sizeof(T));
      if (size == 0) {
        data_ = NULL;
      } else {
        data_ =
            static_cast<T*>(::mmap(NULL, size * sizeof(T),
                                   PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0));
        Warmup((char*) data_, size * sizeof(T));

        ::madvise(data_, size_ * sizeof(T),
                  MMAP_ADVICE_l);  // Turn off readahead
      }
      size_ = size;
    }
  }
#else
  void resize(size_t size) {
    assert(fd_gbp_ != -1);
    if (size == size_) {
      return;
    }

    if (read_only_) {
      if (size < size_) {
        size_ = size;
      } else if (size * sizeof(T) < std::filesystem::file_size(filename_)) {
        size_ = size;
      } else {
        LOG(FATAL)
            << "cannot resize read-only mmap_array to larger size than file";
      }
    } else {
      // auto ret = ftruncate(fd_, size * sizeof(T));
      buffer_pool_manager_->Resize(fd_gbp_, size * sizeof(T));
      size_ = size;
    }
  }
#endif
  bool read_only() const { return read_only_; }

#if OV
  void touch(const std::string& filename) {
    {
      FILE* fout = fopen(filename.c_str(), "wb");
      fwrite(data_, sizeof(T), size_, fout);
      fflush(fout);
      fclose(fout);
    }

    open(filename, false);
  }
#else
  void touch(const std::string& filename) {
    copy_file(filename_, filename);
    open(filename, false);
  }
#endif

#if OV
  T* data() { return data_; }
  const T* data() const { return data_; }
#endif

#if OV
  void set(size_t idx, const T& val) { data_[idx] = val; }
  const T& get(size_t idx) const { return data_[idx]; }
#else
  void set(size_t idx, const T& val, size_t len = 1) {
    CHECK_LE(idx + len, size_);
    // memcpy((char*) (data_ + idx), &val, sizeof(T) * len);

    size_t object_size = sizeof(T) * len;
    buffer_pool_manager_->SetObject(reinterpret_cast<const char*>(&val),
                                    idx * sizeof(T), len * sizeof(T), fd_gbp_);
  }

  void set(size_t idx, const gbp::BufferObject& val, size_t len = 1) {
    CHECK_LE(idx + len, size_);
    auto& val_obj = gbp::Decode<T>(val);
    set(idx, val_obj, len);
  }

  const gbp::BufferObject get(size_t idx, size_t len = 1) const {
    CHECK_LE(idx + len, size_);
    size_t object_size = sizeof(T) * len;
    gbp::BufferObject ret(object_size);
    char* value = ret.Data();

    buffer_pool_manager_->GetObject(ret.Data(), idx * sizeof(T),
                                    len * sizeof(T), fd_gbp_);
    return ret;
  }

#endif

  size_t get_size_in_byte() const { return size_ * sizeof(T); }

#if OV
  T& operator[](size_t idx) { return data_[idx]; }
  const T& operator[](size_t idx) const { return data_[idx]; }
#endif

  size_t size() const { return size_; }

#if OV
  void swap(mmap_array<T>& rhs) {
    std::swap(filename_, rhs.filename_);
    std::swap(fd_, rhs.fd_);
    std::swap(data_, rhs.data_);
    std::swap(size_, rhs.size_);
  }
#else
  void swap(mmap_array<T>& rhs) {
    std::swap(filename_, rhs.filename_);
    std::swap(fd_gbp_, rhs.fd_gbp_);
    std::swap(size_, rhs.size_);
    std::swap(buffer_pool_manager_, rhs.buffer_pool_manager_);
  }
#endif

  const std::string& filename() const { return filename_; }

 private:
#if OV
  void Warmup(char* data, size_t size) {
    static size_t page_num_used = 0;
    if (gbp::get_mark_mmapwarmup().load() == 1) {
      LOG(INFO) << "Warmup file " << filename_;
      volatile int64_t sum = 0;
      for (gbp::page_id offset = 0; offset < size;
           offset += PAGE_SIZE_BUFFER_POOL) {
        sum += data[offset];
        if (++page_num_used == gbp::get_pool_size()) {
          LOG(INFO) << "pool is full";
          return;
        }
      }
    }
  }

  std::string filename_;
  int fd_;
  T* data_;
  size_t size_;
  bool read_only_;
#else

  gbp::BufferPoolManager* buffer_pool_manager_;
  int fd_gbp_ = -1;
  std::string filename_;
  size_t size_;
  bool read_only_;
#endif
};

struct string_item {
  uint64_t offset : 48;
  uint32_t length : 16;
};

template <>
class mmap_array<std::string_view> {
 public:
  mmap_array() {}
  mmap_array(mmap_array&& rhs) : mmap_array() { swap(rhs); }
  ~mmap_array() {}

  void reset() {
    items_.reset();
    data_.reset();
  }

  void open(const std::string& filename, bool read_only) {
    items_.open(filename + ".items", read_only);
    data_.open(filename + ".data", read_only);
  }

  bool read_only() const { return items_.read_only(); }

  void touch(const std::string& filename) {
    items_.touch(filename + ".items");
    data_.touch(filename + ".data");
  }

  void dump(const std::string& filename) {
    items_.dump(filename + ".items");
    data_.dump(filename + ".data");
  }

  void resize(size_t size, size_t data_size) {
    items_.resize(size);
    data_.resize(data_size);
  }
#if OV
  void set(size_t idx, size_t offset, const std::string_view& val) {
    items_.set(idx, {offset, static_cast<uint32_t>(val.size())});
    memcpy(data_.data() + offset, val.data(), val.size());
  }
#else
  void set(size_t idx, size_t offset, const std::string_view& val) {
    items_.set(idx, {offset, static_cast<uint32_t>(val.size())});
    data_.set(offset, *(val.data()), val.size());
    // memcpy(data_.data() + offset, val.data(), val.size());
  }
#endif

#if OV
  std::string_view get(size_t idx) const {
    const string_item& item = items_.get(idx);
    return std::string_view(data_.data() + item.offset, item.length);
  }
#else
  // std::string_view get(size_t idx) const {
  //   const string_item& item = items_.get(idx);
  //   return std::string_view(&data_.get(item.offset), item.length);
  // }
  gbp::BufferObject get(size_t idx) const {
    auto value = items_.get(idx);
    auto item = value.Obj<string_item>();
    return std::move(data_.get(item.offset, item.length));
  }
#endif

  size_t get_size_in_byte() const {
    return items_.size() * sizeof(string_item) + data_.size() * sizeof(char);
  }

  size_t size() const { return items_.size(); }

  size_t data_size() const { return data_.size(); }

  void swap(mmap_array& rhs) {
    items_.swap(rhs.items_);
    data_.swap(rhs.data_);
  }

 private:
  mmap_array<string_item> items_;
  mmap_array<char> data_;
};

}  // namespace gs

#endif  // GRAPHSCOPE_UTILS_MMAP_ARRAY_H_
