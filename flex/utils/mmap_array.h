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

#include <algorithm>
#include <atomic>
#include <filesystem>
#include <string>
#include <string_view>

#include "flex/graphscope_bufferpool/include/buffer_pool_manager.h"
#include "glog/logging.h"

namespace gs {
#define OV true
#define FILE_FLAG O_DIRECT
#define MMAP_ADVICE_l MADV_RANDOM

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
  ::close(src_fd);
  ::close(dst_fd);
}

template <typename T>
class mmap_array {
 public:
#if OV
  mmap_array()
      : filename_(""), fd_(-1), data_(NULL), size_(0), read_only_(true) {}
#else
  mmap_array()
      : filename_(""),
        size_(0),
        read_only_(true),
        fd_gbp_(gbp::INVALID_FILE_HANDLE) {
    buffer_pool_manager_ = &gbp::BufferPoolManager::GetGlobalInstance();
  }
  mmap_array(const mmap_array& other) = delete;
  mmap_array& operator=(const mmap_array&) = delete;
#endif
  mmap_array(mmap_array&& rhs) : mmap_array() { swap(rhs); }
#if OV
  ~mmap_array() {
    if (fd_ != -1) {
      std::vector<std::tuple<void**, int, size_t>>& mas_ = gbp::GetMAS();
      bool mark = true;
      for (size_t i = 0; i < mas_.size(); i++) {
        if (std::get<1>(mas_[i]) == fd_) {
          std::get<1>(mas_[i]) = -1;
          mark = false;
          break;
        }
      }
      if (mark)
        assert(false);
    }
    if (fd_ == -1)
      return;
    size_t file_len = std::filesystem::file_size(filename_);

    size_t total_pages = (file_len + 4095) / 4096;
    unsigned char* vec = (unsigned char*) malloc(total_pages);
    assert(::mincore(data_, file_len, vec) != -1);
    size_t pages_in_memory = 0;
    for (size_t i = 0; i < total_pages; i++) {
      if (vec[i] & 1) {
        pages_in_memory++;
      }
    }
    gbp::get_counter_global(10).fetch_add(pages_in_memory);
    ::free(vec);
  }

  void reset() {
    filename_ = "";
    if (data_ != NULL) {
      ::munmap(data_, size_);
      data_ = NULL;
    }
    if (fd_ != -1) {
      ::close(fd_);
      fd_ = -1;
    }
    read_only_ = true;
  }
#else
  // mmap_array(const mmap_array&) = delete;             // 阻止拷贝
  // mmap_array& operator=(const mmap_array&) = delete;  // 阻止赋值
  ~mmap_array() { close(); }

  void close() {
    // if (fd_gbp_ != gbp::INVALID_FILE_HANDLE) {
    //   buffer_pool_manager_->CloseFile(fd_gbp_);
    //   fd_gbp_ = gbp::INVALID_FILE_HANDLE;
    // }
  }

  void reset() {
    filename_ = "";

    close();
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
        fd_ = ::open(filename.c_str(), O_RDONLY | FILE_FLAG, 0777);

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
      fd_ = ::open(filename.c_str(), O_RDWR | O_CREAT | FILE_FLAG, 0777);
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
    gbp::GetMAS().push_back(std::tuple((void**) (&data_), fd_,
                                       std::filesystem::file_size(filename)));
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
        return;
      } else {
        fd_gbp_ =
            buffer_pool_manager_->OpenFile(filename, O_RDONLY | FILE_FLAG);
      }
    } else {
      fd_gbp_ = buffer_pool_manager_->OpenFile(filename,
                                               O_RDWR | O_CREAT | FILE_FLAG);
    }
    size_t file_size = std::filesystem::file_size(filename);
    size_ = (file_size / gbp::PAGE_SIZE_FILE) * OBJ_NUM_PERPAGE +
            (file_size % gbp::PAGE_SIZE_FILE) / sizeof(T);
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
        // Warmup((char*) data_, size_ * sizeof(T));

        madvise(data_, size_ * sizeof(T),
                MMAP_ADVICE_l);  // Turn off readahead

      } else if (size * sizeof(T) < std::filesystem::file_size(filename_)) {
        munmap(data_, size_ * sizeof(T));
        size_ = size;
        data_ = reinterpret_cast<T*>(
            mmap(NULL, size_ * sizeof(T), PROT_READ, MAP_PRIVATE, fd_, 0));
        // Warmup((char*) data_, size_ * sizeof(T));

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
        // Warmup((char*) data_, size * sizeof(T));

        ::madvise(data_, size * sizeof(T),
                  MMAP_ADVICE_l);  // Turn off readahead
      }
      size_ = size;
    }

    if (fd_ != -1) {
      bool mark = true;
      std::vector<std::tuple<void**, int, size_t>>& mas_ = gbp::GetMAS();
      for (size_t i = 0; i < mas_.size(); i++) {
        if (std::get<1>(mas_[i]) == fd_) {
          std::get<2>(mas_[i]) = size_ * sizeof(T);
          mark = false;
          break;
        }
      }
      if (mark)
        assert(false);
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
      } else if (size < size_) {
        size_ = size;
      } else {
        LOG(FATAL)
            << "cannot resize read-only mmap_array to larger size than file";
      }
    } else {
      size_t file_size_new = (size % OBJ_NUM_PERPAGE) * sizeof(T) +
                             (size / OBJ_NUM_PERPAGE) * gbp::PAGE_SIZE_MEMORY;
      buffer_pool_manager_->Resize(fd_gbp_, file_size_new);
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
    close();
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

  // FIXME: 无法保证atomic，也无法保证单个obj不跨页
  void set(size_t idx, const T* val, size_t len = 1) {
#if ASSERT_ENABLE
    CHECK_LE(idx + len, size_);
#endif
    buffer_pool_manager_->SetBlock(reinterpret_cast<const char*>(val),
                                   idx * sizeof(T), len * sizeof(T), fd_gbp_,
                                   false);
  }

  // // FIXME: 无法保证atomic
  // void set(size_t idx, const gbp::BufferBlock& val) {
  //   const size_t file_offset = idx / OBJ_NUM_PERPAGE * gbp::PAGE_SIZE_FILE +
  //                              (idx % OBJ_NUM_PERPAGE) * sizeof(T);
  //   buffer_pool_manager_->SetObject(val, file_offset, val.Size(), fd_gbp_,
  //                                   false);
  // }

  // const gbp::BufferBlock get(size_t idx, size_t len = 1) const {
  //   CHECK_LE(idx + len, size_);
  //   size_t object_size = sizeof(T) * len;
  //   gbp::BufferBlock ret(object_size);

  //   buffer_pool_manager_->GetObject(ret.Data(), idx * sizeof(T),
  //                                   len * sizeof(T), fd_gbp_);
  //   return ret;
  // }

  const gbp::BufferBlock get(size_t idx, size_t len = 1) const {
#if ASSERT_ENABLE
    CHECK_LE(idx + len, size_);
#endif

    size_t buf_size = 0;
    const size_t file_offset = idx / OBJ_NUM_PERPAGE * gbp::PAGE_SIZE_FILE +
                               (idx % OBJ_NUM_PERPAGE) * sizeof(T);
    size_t rest_filelen_firstpage =
        gbp::PAGE_SIZE_MEMORY - file_offset % gbp::PAGE_SIZE_MEMORY;
    if (rest_filelen_firstpage / sizeof(T) > len) {
      buf_size += sizeof(T) * len;
    } else {
      buf_size += rest_filelen_firstpage;
      len -= rest_filelen_firstpage / sizeof(T);
      buf_size += len / OBJ_NUM_PERPAGE * gbp::PAGE_SIZE_MEMORY +
                  len % OBJ_NUM_PERPAGE * sizeof(T);
    }
    auto ret = buffer_pool_manager_->GetBlock(file_offset, buf_size, fd_gbp_);

    return ret;
  }

#endif

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
    rhs.fd_ = -1;
  }
  size_t get_size_in_byte() const { return size_ * sizeof(T); }
#else
  void swap(mmap_array<T>& rhs) {
    std::swap(filename_, rhs.filename_);
    std::swap(fd_gbp_, rhs.fd_gbp_);
    std::swap(size_, rhs.size_);
    std::swap(buffer_pool_manager_, rhs.buffer_pool_manager_);
    rhs.fd_gbp_ = gbp::INVALID_FILE_HANDLE;
  }

  gbp::GBPfile_handle_type filehandle() const { return fd_gbp_; }
  size_t get_size_in_byte() const {
    return (size_ % OBJ_NUM_PERPAGE) * sizeof(T) +
           (size_ / OBJ_NUM_PERPAGE) * gbp::PAGE_SIZE_MEMORY;
  }
#endif

  const std::string& filename() const { return filename_; }

 private:
  mutable bool mark_used_ = false;
#if OV
  void Warmup(char* data, size_t size) {
    static size_t page_num_used = 0;
    if (gbp::warmup_mark().load() == 1) {
      LOG(INFO) << "Warmup file " << filename_;
      volatile int64_t sum = 0;
      for (size_t offset = 0; offset < size; offset += 4096) {
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
  mutable bool restart_finish_ = false;
#else
  constexpr static uint16_t OBJ_NUM_PERPAGE = gbp::PAGE_SIZE_FILE / sizeof(T);
  gbp::BufferPoolManager* buffer_pool_manager_;
  gbp::GBPfile_handle_type fd_gbp_ = gbp::INVALID_FILE_HANDLE;
  std::string filename_;
  size_t size_;
  bool read_only_;
  mutable bool restart_finish_ = false;
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
    string_item string_item_obj = {offset, static_cast<uint32_t>(val.size())};
    items_.set(idx, &string_item_obj);
    data_.set(offset, val.data(), val.size());
  }
#endif

#if OV
  std::string_view get(size_t idx) const {
    const string_item& item = items_.get(idx);
    return std::string_view(data_.data() + item.offset, item.length);
  }
#else
  gbp::BufferBlock get(size_t idx) const {
    auto value = items_.get(idx);
    auto& item = gbp::BufferBlock::Ref<gs::string_item>(value);
    auto ret = data_.get(item.offset, item.length);
    value.free();
    return ret;
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
