#pragma once

#include <cstddef>
#include <vector>

#include "column_family.h"
#include "flex/graphscope_bufferpool/include/mmap_array.h"
#include "flex/storages/rt_mutable_graph/types.h"
#include "flex/utils/mmap_array.h"
#include "flex/utils/property/types.h"
#include "grape/io/local_io_adaptor.h"
#include "grape/serialization/in_archive.h"
#include "grape/serialization/out_archive.h"
#include "grape/types.h"

namespace gs {
namespace cgraph {
class EdgeHandle {
 public:
  EdgeHandle() = default;
  EdgeHandle(ColumnHandle adj_list, mmap_array_base* csr,
             PropertyType edge_type)
      : adj_list_(adj_list), csr_(csr), edge_type_(edge_type) {}
  ~EdgeHandle() = default;
  FORCE_INLINE gbp::BufferBlock getEdges(vid_t v_id, size_t& edge_size) const {
    switch (edge_type_) {
    case PropertyType::kDynamicEdgeList: {
      auto adj_item = adj_list_.getColumn(v_id);
      auto& adj_list = gbp::BufferBlock::Ref<MutableAdjlist>(adj_item);
      edge_size = adj_list.size_;
      return csr_->get(adj_list.start_idx_, adj_list.size_);
    }
    case PropertyType::kEdge: {
      edge_size = 1;
      return adj_list_.getColumn(v_id);
    }
    default: {
      assert(false);
      return gbp::BufferBlock();
    }
    }
  }

 private:
  ColumnHandle adj_list_;
  const mmap_array_base* csr_;
  const PropertyType edge_type_;
};

class PropertyHandle {
 public:
  PropertyHandle(ColumnHandle column, const mmap_array<char>* stringpool,
                 const PropertyType property_type)
      : column_(column),
        stringpool_(stringpool),
        property_type_(property_type) {}
  ~PropertyHandle() = default;

  FORCE_INLINE gbp::BufferBlock getProperty(vid_t v_id) const {
#if PROFILE_ENABLE
    auto start = gbp::GetSystemTime();
#endif
    switch (property_type_) {
    case gs::PropertyType::kInt32:
    case gs::PropertyType::kDate:
    case gs::PropertyType::kInt64: {
      auto ret = column_.getColumn(v_id);
#if PROFILE_ENABLE
      auto end = gbp::GetSystemTime();
      gbp::get_counter(7) += end - start;
      gbp::get_counter(8) += 1;
#endif
      return ret;
    }
    case PropertyType::kString: {
      auto position = column_.getColumn(v_id);
      auto& item = gbp::BufferBlock::Ref<string_item>(position);
      auto ret = stringpool_->get(item.offset, item.length);
#if PROFILE_ENABLE
      auto end = gbp::GetSystemTime();
      gbp::get_counter(7) += end - start;
      gbp::get_counter(8) += 1;
#endif
      return ret;
    }
    default: {
      assert(false);
    }
    }
  }

 private:
  ColumnHandle column_;
  const mmap_array<char>* stringpool_;
  const PropertyType property_type_;
};

inline void InsertEdgeAtomicHelper(gbp::BufferBlock edges,
                                   std::string_view e_property,
                                   size_t e_neighbor,
                                   PropertyType e_property_type,
                                   size_t timestamp, size_t idx_in_edges = 0) {
  switch (e_property_type) {
  case PropertyType::kEmpty: {
    gbp::BufferBlock::UpdateContent<MutableNbr<grape::EmptyType>>(
        [&](MutableNbr<grape::EmptyType>& item) {
          item.neighbor = e_neighbor;
          item.timestamp.store(timestamp);
        },
        edges, idx_in_edges);
    break;
  }
  case PropertyType::kDate: {
    gbp::BufferBlock::UpdateContent<MutableNbr<gs::Date>>(
        [&](MutableNbr<gs::Date>& item) {
          item.neighbor = e_neighbor;
          item.data = *reinterpret_cast<const gs::Date*>(e_property.data());
          item.timestamp.store(timestamp);
        },
        edges, idx_in_edges);
    break;
  }
  case PropertyType::kInt32: {
    gbp::BufferBlock::UpdateContent<MutableNbr<int32_t>>(
        [&](MutableNbr<int32_t>& item) {
          item.neighbor = e_neighbor;
          item.data = *reinterpret_cast<const int32_t*>(e_property.data());
          item.timestamp.store(timestamp);
        },
        edges, idx_in_edges);
    break;
  }
  case PropertyType::kInt64: {
    gbp::BufferBlock::UpdateContent<MutableNbr<int64_t>>(
        [&](MutableNbr<int64_t>& item) {
          item.neighbor = e_neighbor;
          item.data = *reinterpret_cast<const int64_t*>(e_property.data());
          item.timestamp.store(timestamp);
        },
        edges, idx_in_edges);
    break;
  }
  case PropertyType::kDouble: {
    gbp::BufferBlock::UpdateContent<MutableNbr<double>>(
        [&](MutableNbr<double>& item) {
          item.neighbor = e_neighbor;
          item.data = *reinterpret_cast<const double*>(e_property.data());
          item.timestamp.store(timestamp);
        },
        edges, idx_in_edges);
    break;
  }
  default:
    assert(false);
  }
}

// template <typename PROPERTY_TYPE>
// void InsertEdgeAtomicHelper(gbp::BufferBlock edges, PROPERTY_TYPE e_property,
//                             size_t e_neighbor, size_t e_timestamp);

template <class PROPERTY_TYPE>
inline void InsertEdgeAtomicHelper(gbp::BufferBlock edges,
                                   PROPERTY_TYPE e_property, size_t e_neighbor,
                                   size_t e_timestamp) {
  gbp::BufferBlock::UpdateContent<MutableNbr<PROPERTY_TYPE>>(
      [&](auto& item) {
        item.neighbor = e_neighbor;
        item.data = e_property;
        item.timestamp.store(e_timestamp);
      },
      edges);
}

template <>
inline void InsertEdgeAtomicHelper(gbp::BufferBlock edges,
                                   grape::EmptyType e_property,
                                   size_t e_neighbor, size_t e_timestamp) {
  gbp::BufferBlock::UpdateContent<MutableNbr<grape::EmptyType>>(
      [&](auto& item) {
        item.neighbor = e_neighbor;
        item.timestamp.store(e_timestamp);
      },
      edges);
}

class Vertex {
 public:
  enum EdgeDirection { kIn, kOut, kNone };
  class DataPerColumnFamily {
   public:
    DataPerColumnFamily(bool empty = false) {
      if (empty) {
        fixed_length_column_family = nullptr;
        stringpool = nullptr;
        stringpool_size = 0;
        csr.clear();
      } else {
        fixed_length_column_family = new FixedLengthColumnFamily();
        stringpool = new mmap_array<char>();
        stringpool_size = 0;
        csr.clear();
      }
    }
    // // 删除拷贝构造和赋值
    // DataPerColumnFamily(const DataPerColumnFamily&) = delete;
    // DataPerColumnFamily& operator=(const DataPerColumnFamily&) = delete;
    // // 实现移动语义
    // DataPerColumnFamily(DataPerColumnFamily&& other) noexcept = default;
    // DataPerColumnFamily& operator=(DataPerColumnFamily&& other) noexcept =
    //     default;

    // FIXME: 这里不太安全，几个指针未被安全释放
    ~DataPerColumnFamily() {
      // if (fixed_length_column_family != nullptr) {
      //   delete fixed_length_column_family;
      //   fixed_length_column_family = nullptr;
      // }
      // if (dynamic_length_column_family != nullptr) {
      //   delete dynamic_length_column_family;
      //   dynamic_length_column_family = NULL;
      // }
      // for (auto& csr : csr) {
      //   delete csr;
      // }
    }
    FixedLengthColumnFamily* fixed_length_column_family = nullptr;
    mmap_array<char>* stringpool = nullptr;
    size_t stringpool_size = 0;
    std::vector<mmap_array_base*>
        csr;  // csr之所以是一个vector，是因为一个vertex可能有多个edge_list，不同的edge的size可能不同，放在同一个mmap_array中不便于对齐
  };

  struct ColumnConfiguration {
    size_t property_id;
    gs::PropertyType column_type;
    size_t column_family_id;
    PropertyType edge_type;
    std::string column_name;
    std::string nbr_name;
    EdgeDirection nbr_direction;

    size_t column_id_in_column_family;
    size_t edge_list_id_in_column_family;

    ColumnConfiguration() = default;
    ColumnConfiguration(size_t property_id_in, gs::PropertyType column_type_in,
                        size_t column_family_id_in, PropertyType edge_type_in,
                        const std::string& column_name_in,
                        const std::string& nbr_name_in,
                        EdgeDirection nbr_direction_in)
        : property_id(property_id_in),
          column_type(column_type_in),
          edge_type(edge_type_in),
          column_family_id(column_family_id_in),
          column_name(column_name_in),
          nbr_name(nbr_name_in),
          nbr_direction(nbr_direction_in) {}
    ~ColumnConfiguration() = default;
    void print() const {
      LOG(INFO) << "property_id: " << property_id
                << " column_type: " << static_cast<int>(column_type)
                << " column_family_id: " << column_family_id
                << " edge_type: " << static_cast<int>(edge_type)
                << " column_name: " << column_name << " nbr_name: " << nbr_name
                << " nbr_direction: " << nbr_direction
                << " column_id_in_column_family: " << column_id_in_column_family
                << " edge_list_id_in_column_family: "
                << edge_list_id_in_column_family;
    }

    inline grape::InArchive& operator<<(grape::InArchive& in_archive) {
      in_archive << property_id;
      in_archive << column_type;
      in_archive << column_family_id;
      in_archive << edge_type;
      in_archive << column_name;
      in_archive << nbr_name;
      in_archive << nbr_direction;
      in_archive << column_id_in_column_family;
      in_archive << edge_list_id_in_column_family;
      return in_archive;
    }

    // 清晰的格式化输出
    friend grape::InArchive& operator<<(grape::InArchive& os,
                                        const ColumnConfiguration& obj) {
      // 使用适当的缩进和格式
      os << obj.property_id;
      os << obj.column_type;
      os << obj.column_family_id;
      os << obj.edge_type;
      os << obj.column_name;
      os << obj.nbr_name;
      os << obj.nbr_direction;
      os << obj.column_id_in_column_family;
      os << obj.edge_list_id_in_column_family;
      return os;
    }
    friend grape::OutArchive& operator>>(grape::OutArchive& os,
                                         ColumnConfiguration& obj) {
      os >> obj.property_id;
      os >> obj.column_type;
      os >> obj.column_family_id;
      os >> obj.edge_type;
      os >> obj.column_name;
      os >> obj.nbr_name;
      os >> obj.nbr_direction;
      os >> obj.column_id_in_column_family;
      os >> obj.edge_list_id_in_column_family;
      return os;
    }
  };

  Vertex() = default;
  ~Vertex() { Close(); }
  // tuple<property_id, column_family_id, column_type, edge_type>

  void Open(const std::string& vertex_name, const std::string& db_dir_path) {
    db_dir_path_ = db_dir_path;
    // 打开metadata文件
    auto io_adaptor =
        std::unique_ptr<grape::LocalIOAdaptor>(new grape::LocalIOAdaptor(
            db_dir_path_ + "/" + vertex_name + "/metadata"));
    io_adaptor->Open();
    Deserialize(io_adaptor);
    assert(vertex_name_ == vertex_name);

    // 初始化column family
    datas_of_all_column_family_.reserve(column_family_num_);
    for (auto column_family_id = 0; column_family_id < column_family_num_;
         column_family_id++) {
      if (column_family_info_[column_family_id].column_number == 0) {
        datas_of_all_column_family_.emplace_back(true);
        continue;
      }
      datas_of_all_column_family_.emplace_back();

      datas_of_all_column_family_[column_family_id].csr.resize(
          column_family_info_[column_family_id]
              .edge_list_sizes_.size());  // 初始化csr
      std::vector<size_t> column_lengths(
          column_family_info_[column_family_id]
              .column_number);  // 初始化column_lengths
      bool string_mark = false;
      for (auto& column_configuration :
           property_id_to_ColumnToColumnFamily_configurations_) {
        // 如果column_configuration不属于当前column_family，则跳过
        if (column_configuration.second.column_family_id != column_family_id) {
          continue;
        }
        // 根据column的类型，初始化column family
        switch (column_configuration.second.column_type) {
        case gs::PropertyType::kInt32:
          column_lengths[column_configuration.second
                             .column_id_in_column_family] = sizeof(int32_t);
          break;
        case gs::PropertyType::kDate:
          column_lengths[column_configuration.second
                             .column_id_in_column_family] = sizeof(gs::Date);
          break;
        case gs::PropertyType::kInt64: {
          column_lengths[column_configuration.second
                             .column_id_in_column_family] = sizeof(int64_t);
          break;
        }
        case gs::PropertyType::kString: {
          column_lengths[column_configuration.second
                             .column_id_in_column_family] = sizeof(string_item);
          string_mark = true;
          break;
        }
        case gs::PropertyType::kDouble: {
          column_lengths[column_configuration.second
                             .column_id_in_column_family] = sizeof(double);
          break;
        }
        case gs::PropertyType::kEdge:
        case gs::PropertyType::kDynamicEdgeList: {
          if (column_configuration.second.column_type ==
              gs::PropertyType::kDynamicEdgeList) {
            column_lengths[column_configuration.second
                               .column_id_in_column_family] =
                sizeof(MutableAdjlist);
          }
          switch (column_configuration.second.edge_type) {
          case PropertyType::kInt32: {
            if (column_configuration.second.column_type ==
                gs::PropertyType::kDynamicEdgeList) {
              auto mmap_array_ptr = new mmap_array<MutableNbr<int32_t>>();
              mmap_array_ptr->open(
                  db_dir_path_ + "/" + vertex_name_ + "/column_family_" +
                      std::to_string(column_family_id) + "_" +
                      column_configuration.second.column_name + "_" +
                      column_configuration.second.nbr_name + "_" +
                      std::to_string(static_cast<int>(
                          column_configuration.second.nbr_direction)) +
                      ".edgelist",
                  false);
              datas_of_all_column_family_[column_family_id]
                  .csr[column_configuration.second
                           .edge_list_id_in_column_family] = mmap_array_ptr;
            } else if (column_configuration.second.column_type ==
                       gs::PropertyType::kEdge) {
              column_lengths[column_configuration.second
                                 .column_id_in_column_family] =
                  sizeof(MutableNbr<int32_t>);
            } else {
              assert(false);
            }
            break;
          }
          case PropertyType::kDate: {
            if (column_configuration.second.column_type ==
                gs::PropertyType::kDynamicEdgeList) {
              auto mmap_array_ptr = new mmap_array<MutableNbr<gs::Date>>();
              mmap_array_ptr->open(
                  db_dir_path_ + "/" + vertex_name_ + "/column_family_" +
                      std::to_string(column_family_id) + "_" +
                      column_configuration.second.column_name + "_" +
                      column_configuration.second.nbr_name + "_" +
                      std::to_string(static_cast<int>(
                          column_configuration.second.nbr_direction)) +
                      ".edgelist",
                  false);
              datas_of_all_column_family_[column_family_id]
                  .csr[column_configuration.second
                           .edge_list_id_in_column_family] = mmap_array_ptr;
            } else if (column_configuration.second.column_type ==
                       gs::PropertyType::kEdge) {
              column_lengths[column_configuration.second
                                 .column_id_in_column_family] =
                  sizeof(MutableNbr<gs::Date>);
            } else {
              assert(false);
            }
            break;
          }
          case PropertyType::kEmpty: {
            if (column_configuration.second.column_type ==
                gs::PropertyType::kDynamicEdgeList) {
              auto mmap_array_ptr =
                  new mmap_array<MutableNbr<grape::EmptyType>>();
              mmap_array_ptr->open(
                  db_dir_path_ + "/" + vertex_name_ + "/column_family_" +
                      std::to_string(column_family_id) + "_" +
                      column_configuration.second.column_name + "_" +
                      column_configuration.second.nbr_name + "_" +
                      std::to_string(static_cast<int>(
                          column_configuration.second.nbr_direction)) +
                      ".edgelist",
                  false);
              datas_of_all_column_family_[column_family_id]
                  .csr[column_configuration.second
                           .edge_list_id_in_column_family] = mmap_array_ptr;
            } else if (column_configuration.second.column_type ==
                       gs::PropertyType::kEdge) {
              column_lengths[column_configuration.second
                                 .column_id_in_column_family] =
                  sizeof(MutableNbr<grape::EmptyType>);
            } else {
              assert(false);
            }
            break;
          }
          default: {
            assert(false);
          }
          }
          break;
        }
        }
      }
      // 初始化固定长度的column family
      datas_of_all_column_family_[column_family_id]
          .fixed_length_column_family->Open(
              column_lengths,
              db_dir_path_ + "/" + vertex_name_ + "/column_family_" +
                  std::to_string(column_family_id) + ".property");

      // 初始化存储string的文件
      if (string_mark) {
        datas_of_all_column_family_[column_family_id].stringpool->open(
            db_dir_path_ + "/" + vertex_name_ + "/column_family_" +
                std::to_string(column_family_id) + ".stringpool",
            false);
      }
    }
  }

  void Close() {
    auto io_adaptor =
        std::unique_ptr<grape::LocalIOAdaptor>(new grape::LocalIOAdaptor(
            db_dir_path_ + "/" + vertex_name_ + "/metadata"));
    io_adaptor->Open("wb");
    Serialize(io_adaptor);
    io_adaptor->Close();
  }

  void Init(
      std::vector<std::vector<ColumnConfiguration>>& column_configurations,
      std::map<size_t, size_t>& edge_label_to_property_id,
      const std::string& vertex_name, const std::string& db_dir_path) {
    edge_label_to_property_id_ = edge_label_to_property_id;
    db_dir_path_ = db_dir_path;
    {
      vertex_name_ = vertex_name;
      std::filesystem::create_directory(db_dir_path_ + "/" + vertex_name_);
    }
    column_family_num_ = column_configurations.size();
    datas_of_all_column_family_.reserve(column_family_num_);
    auto column_family_id = 0;
    for (auto& column_configurations_tmp : column_configurations) {
      column_family_info_.emplace_back(column_configurations_tmp.size());
      // 初始化每一个Column Family
      std::vector<size_t> column_lengths;  // 每个column的长度
      auto string_mark = false;            // 是否有string类型的column
      if (column_configurations_tmp.size() == 0) {
        column_family_id++;
        datas_of_all_column_family_.emplace_back(true);
        continue;
      }
      datas_of_all_column_family_.emplace_back();

      // 初始化每一个Column
      auto edge_list_column_count = 0;
      auto column_id_in_column_family = 0;  // 在column family中的id
      for (auto& column_configuration : column_configurations_tmp) {
        // 记录property_id和column_to_column_family的映射
        if (property_id_to_ColumnToColumnFamily_configurations_.count(
                column_configuration.property_id) != 0) {
          assert(false);
        }
        property_id_to_ColumnToColumnFamily_configurations_.insert(
            {column_configuration.property_id, column_configuration});

        property_id_to_ColumnToColumnFamily_configurations_[column_configuration
                                                                .property_id]
            .column_id_in_column_family = column_id_in_column_family++;
        // 根据column的类型，初始化column family
        switch (column_configuration.column_type) {
        case gs::PropertyType::kInt32:
          column_lengths.push_back(sizeof(int32_t));
          break;
        case gs::PropertyType::kDate:
          column_lengths.push_back(sizeof(gs::Date));
          break;
        case gs::PropertyType::kInt64: {
          column_lengths.push_back(sizeof(int64_t));
          break;
        }
        case gs::PropertyType::kString: {
          column_lengths.push_back(sizeof(string_item));
          string_mark = true;
          break;
        }
        case gs::PropertyType::kDouble: {
          column_lengths.push_back(sizeof(double));
          break;
        }
        case gs::PropertyType::kEdge:
        case gs::PropertyType::kDynamicEdgeList: {
          if (column_configuration.column_type ==
              gs::PropertyType::kDynamicEdgeList) {
            column_lengths.push_back(sizeof(MutableAdjlist));
          }
          switch (column_configuration.edge_type) {
          case PropertyType::kInt32: {
            if (column_configuration.column_type ==
                gs::PropertyType::kDynamicEdgeList) {
              auto mmap_array_ptr = new mmap_array<MutableNbr<int32_t>>();
              mmap_array_ptr->open(
                  db_dir_path_ + "/" + vertex_name_ + "/column_family_" +
                      std::to_string(column_family_id) + "_" +
                      column_configuration.column_name + "_" +
                      column_configuration.nbr_name + "_" +
                      std::to_string(static_cast<int>(
                          column_configuration.nbr_direction)) +
                      ".edgelist",
                  false);
              datas_of_all_column_family_[column_family_id].csr.emplace_back(
                  mmap_array_ptr);
            } else if (column_configuration.column_type ==
                       gs::PropertyType::kEdge) {
              column_lengths.push_back(sizeof(MutableNbr<int32_t>));
            } else {
              assert(false);
            }
            break;
          }
          case PropertyType::kDate: {
            if (column_configuration.column_type ==
                gs::PropertyType::kDynamicEdgeList) {
              auto mmap_array_ptr = new mmap_array<MutableNbr<gs::Date>>();
              mmap_array_ptr->open(
                  db_dir_path_ + "/" + vertex_name_ + "/column_family_" +
                      std::to_string(column_family_id) + "_" +
                      column_configuration.column_name + "_" +
                      column_configuration.nbr_name + "_" +
                      std::to_string(static_cast<int>(
                          column_configuration.nbr_direction)) +
                      ".edgelist",
                  false);
              datas_of_all_column_family_[column_family_id].csr.emplace_back(
                  mmap_array_ptr);
            } else if (column_configuration.column_type ==
                       gs::PropertyType::kEdge) {
              column_lengths.push_back(sizeof(MutableNbr<gs::Date>));
            } else {
              assert(false);
            }
            break;
          }
          case PropertyType::kEmpty: {
            if (column_configuration.column_type ==
                gs::PropertyType::kDynamicEdgeList) {
              auto mmap_array_ptr =
                  new mmap_array<MutableNbr<grape::EmptyType>>();
              mmap_array_ptr->open(
                  db_dir_path_ + "/" + vertex_name_ + "/column_family_" +
                      std::to_string(column_family_id) + "_" +
                      column_configuration.column_name + "_" +
                      column_configuration.nbr_name + "_" +
                      std::to_string(static_cast<int>(
                          column_configuration.nbr_direction)) +
                      ".edgelist",
                  false);
              datas_of_all_column_family_[column_family_id].csr.emplace_back(
                  mmap_array_ptr);
            } else if (column_configuration.column_type ==
                       gs::PropertyType::kEdge) {
              column_lengths.push_back(sizeof(MutableNbr<grape::EmptyType>));
            } else {
              assert(false);
            }
            break;
          }
          default: {
            assert(false);
          }
          }
          if (column_configuration.column_type ==
              gs::PropertyType::kDynamicEdgeList) {
            property_id_to_ColumnToColumnFamily_configurations_
                [column_configuration.property_id]
                    .edge_list_id_in_column_family = edge_list_column_count++;
            column_family_info_[column_family_id].edge_list_sizes_.push_back(0);
          }
          break;
        }
        }
      }
      // 初始化固定长度的column family
      datas_of_all_column_family_[column_family_id]
          .fixed_length_column_family->Open(
              column_lengths,
              db_dir_path_ + "/" + vertex_name_ + "/column_family_" +
                  std::to_string(column_family_id) + ".property");

      // 初始化存储string的文件
      if (string_mark) {
        datas_of_all_column_family_[column_family_id].stringpool->open(
            db_dir_path_ + "/" + vertex_name_ + "/column_family_" +
                std::to_string(column_family_id) + ".stringpool",
            false);
      }

      column_family_id++;
    }
  }

  void InsertVertex(
      size_t vertex_id,
      const std::vector<std::pair<size_t, std::string_view>>& values) {
    for (auto& value : values) {
      InsertColumn(vertex_id, value);
    }
  }

  void InsertColumn(size_t vertex_id,
                    const std::pair<size_t, std::string_view> value) {
#if ASSERT_ENABLE
    assert(property_id_to_ColumnToColumnFamily_configurations_.count(
               value.first) == 1);
#endif

    auto column_to_column_family =
        property_id_to_ColumnToColumnFamily_configurations_[value.first];

    switch (column_to_column_family.column_type) {
    case gs::PropertyType::kInt32:
    case gs::PropertyType::kDate:
    case gs::PropertyType::kInt64: {
      datas_of_all_column_family_[column_to_column_family.column_family_id]
          .fixed_length_column_family->setColumn(
              vertex_id, column_to_column_family.column_id_in_column_family,
              value.second);
      break;
    }
    case gs::PropertyType::kString:
    case gs::PropertyType::kDynamicString: {
      // 获得start_pos
      auto start_pos =
          gbp::as_atomic(datas_of_all_column_family_[column_to_column_family
                                                         .column_family_id]
                             .stringpool_size)
              .fetch_add(value.second.size());

      // 存储string内容
      datas_of_all_column_family_[column_to_column_family.column_family_id]
          .stringpool->set(start_pos, value.second, value.second.size());

      // 存储string的position
      string_item position = {start_pos, (uint32_t) value.second.size()};
      datas_of_all_column_family_[column_to_column_family.column_family_id]
          .fixed_length_column_family->setColumn(
              vertex_id, column_to_column_family.column_id_in_column_family,
              {reinterpret_cast<char*>(&position), sizeof(string_item)});
      break;
    }
    case gs::PropertyType::kEdge:
    case gs::PropertyType::kDynamicEdgeList: {
      assert(false);
      break;
    }
    default: {
      assert(false);
    }
    }
  }
  void EdgeListInit(size_t edge_label_id, size_t vertex_id, size_t degree_max) {
#if ASSERT_ENABLE
    assert(edge_label_to_property_id_.count(edge_label_id) == 1);
#endif

    auto column_to_column_family =
        property_id_to_ColumnToColumnFamily_configurations_
            [edge_label_to_property_id_[edge_label_id]];

    auto item_t =
        datas_of_all_column_family_[column_to_column_family.column_family_id]
            .fixed_length_column_family->getColumn(
                vertex_id, column_to_column_family.column_id_in_column_family);
    gbp::BufferBlock::UpdateContent<MutableAdjlist>(
        [&](MutableAdjlist& item) {
          // 获得锁
          assert(item.start_idx_ == 0);
          assert(item.capacity_ == 0);
          assert(item.size_ == 0);
          assert(item.lock_.load() == 0);
          item.start_idx_ =
              gbp::as_atomic(
                  column_family_info_[column_to_column_family.column_family_id]
                      .edge_list_sizes_[column_to_column_family
                                            .edge_list_id_in_column_family])
                  .fetch_add(degree_max);
          // LOG(INFO)<<"start_idx_: "<<item.start_idx_;

          item.capacity_ = degree_max;
          item.size_ = 0;
        },
        item_t);
  }

  // 1. 初始化所有的单边
  // 2. 初始化所有的多边：为多边分配空间
  void InitVertex(size_t vertex_id) {
    // 初始化所有边
    std::vector<char> empty_edge;
    for (auto& edge_label_id : edge_label_to_property_id_) {
      auto column_to_column_family =
          property_id_to_ColumnToColumnFamily_configurations_[edge_label_id
                                                                  .second];
      if (column_to_column_family.column_type ==
          gs::PropertyType::kDynamicEdgeList) {
        EdgeListInit(edge_label_id.first, vertex_id, 4);
      } else if (column_to_column_family.column_type ==
                 gs::PropertyType::kEdge) {
        assert(ConstructEdgeNew(empty_edge, std::string(), 0,
                                column_to_column_family.edge_type,
                                std::numeric_limits<timestamp_t>::max(), true));

        datas_of_all_column_family_[column_to_column_family.column_family_id]
            .fixed_length_column_family->setColumn(
                vertex_id, column_to_column_family.column_id_in_column_family,
                std::string_view(empty_edge.data(), empty_edge.size()));
      }
    }
  }

  void EdgeListInitBatch(size_t edge_label_id,
                         std::vector<std::pair<size_t, size_t>>& values) {
#if ASSERT_ENABLE
    assert(edge_label_to_property_id_.count(edge_label_id) == 1);
#endif

    auto column_to_column_family =
        property_id_to_ColumnToColumnFamily_configurations_
            [edge_label_to_property_id_[edge_label_id]];
    if (column_to_column_family.column_type !=
        gs::PropertyType::kDynamicEdgeList) {
      assert(false);
    }
    for (auto& value : values) {
      EdgeListInit(edge_label_id, value.first, value.second);
    }
    datas_of_all_column_family_[column_to_column_family.column_family_id]
        .csr[column_to_column_family.edge_list_id_in_column_family]
        ->resize(column_family_info_[column_to_column_family.column_family_id]
                     .edge_list_sizes_[column_to_column_family
                                           .edge_list_id_in_column_family] *
                 10);
  }
  template <typename PROPERTY_TYPE>
  void InsertEdgeConcurrent(size_t vertex_id, size_t edge_label_id,
                            PROPERTY_TYPE property, size_t neighbor,
                            timestamp_t timestamp) {
    auto column_to_column_family =
        property_id_to_ColumnToColumnFamily_configurations_
            [edge_label_to_property_id_[edge_label_id]];

    switch (column_to_column_family.column_type) {
    case gs::PropertyType::kDynamicEdgeList: {
      auto adj_list_item =
          datas_of_all_column_family_[column_to_column_family.column_family_id]
              .fixed_length_column_family->getColumn(
                  vertex_id,
                  column_to_column_family.column_id_in_column_family);
      size_t idx_new = 0;

      gbp::BufferBlock::UpdateContent<MutableAdjlist>(
          [&](MutableAdjlist& item) {
            u_int8_t old_data;
            do {
              old_data = 0;
            } while (!item.lock_.compare_exchange_weak(
                old_data, 1, std::memory_order_release,
                std::memory_order_relaxed));  // 获得锁,锁住整个edge list的修改
            idx_new = item.size_;  // 获得当前边的相对插入位置
#if ASSERT_ENABLE
            assert(item.capacity_ >= idx_new);
#endif
            if (item.capacity_ == idx_new) {
              item.capacity_ = item.capacity_ == 0 ? 4 : item.capacity_ * 2;
              auto new_start_idx =
                  gbp::as_atomic(
                      column_family_info_[column_to_column_family
                                              .column_family_id]
                          .edge_list_sizes_[column_to_column_family
                                                .edge_list_id_in_column_family])
                      .fetch_add(item.capacity_);  // 分配空间
#if ASSERT_ENABLE
              auto new_capacity =
                  column_family_info_[column_to_column_family.column_family_id]
                      .edge_list_sizes_[column_to_column_family
                                            .edge_list_id_in_column_family];
              auto size_aaa =
                  datas_of_all_column_family_[column_to_column_family
                                                  .column_family_id]
                      .csr[column_to_column_family
                               .edge_list_id_in_column_family]
                      ->size();
              if (new_capacity > size_aaa) {
                LOG(INFO) << "new_capacity: " << new_capacity << " "
                          << size_aaa;
              }
              assert(new_capacity <= size_aaa);
#endif
              {  // 复制旧数据
                auto nbr_slice_old =
                    datas_of_all_column_family_[column_to_column_family
                                                    .column_family_id]
                        .csr[column_to_column_family
                                 .edge_list_id_in_column_family]
                        ->get(item.start_idx_, item.size_.load());
                auto nbr_slice_new =
                    datas_of_all_column_family_[column_to_column_family
                                                    .column_family_id]
                        .csr[column_to_column_family
                                 .edge_list_id_in_column_family]
                        ->get(new_start_idx, item.size_.load());
                for (size_t i = 0; i < item.size_.load(); i++) {
                  gbp::BufferBlock::UpdateContent<MutableNbr<PROPERTY_TYPE>>(
                      [&](MutableNbr<PROPERTY_TYPE>& item) {
                        auto& item_old =
                            gbp::BufferBlock::Ref<MutableNbr<PROPERTY_TYPE>>(
                                nbr_slice_old, i);
                        item.data = item_old.data;
                        item.neighbor = item_old.neighbor;
                        item.timestamp = item_old.timestamp.load();
                      },
                      nbr_slice_new, i);
                }
              }
              item.start_idx_ = new_start_idx;  // 更新start_idx_
            }
            idx_new += item.start_idx_;  // 获得当前边的绝对插入位置

            // 插入边
            auto nbr_item =
                datas_of_all_column_family_[column_to_column_family
                                                .column_family_id]
                    .csr[column_to_column_family.edge_list_id_in_column_family]
                    ->get(idx_new);
            InsertEdgeAtomicHelper(nbr_item, property, neighbor, timestamp);

            item.size_++;
            item.lock_.store(0);  // 释放锁
          },
          adj_list_item);
      break;
    }
    case gs::PropertyType::kEdge: {
      auto edge_item =
          datas_of_all_column_family_[column_to_column_family.column_family_id]
              .fixed_length_column_family->getColumn(
                  vertex_id,
                  column_to_column_family.column_id_in_column_family);

      InsertEdgeAtomicHelper(edge_item, property, neighbor, timestamp);
      break;
    }
    default: {
      LOG(INFO) << "column_to_column_family.column_name: "
                << static_cast<int>(column_to_column_family.column_type);
      assert(false);
    }
    }
  }

  // pair<property_id, value>
  void InsertEdgeUnsafe(size_t vertex_id,
                        std::pair<size_t, std::string_view> value) {
#if ASSERT_ENABLE
    assert(edge_label_to_property_id_.count(value.first) == 1);
#endif
    auto column_to_column_family =
        property_id_to_ColumnToColumnFamily_configurations_
            [edge_label_to_property_id_[value.first]];
    switch (column_to_column_family.column_type) {
    case gs::PropertyType::kDynamicEdgeList: {
      auto adj_list_item =
          datas_of_all_column_family_[column_to_column_family.column_family_id]
              .fixed_length_column_family->getColumn(
                  vertex_id,
                  column_to_column_family.column_id_in_column_family);
      size_t idx_new = 0;

      gbp::BufferBlock::UpdateContent<MutableAdjlist>(
          [&](MutableAdjlist& item) {
            u_int8_t old_data;
            do {
              old_data = 0;
            } while (!item.lock_.compare_exchange_weak(
                old_data, 1, std::memory_order_release,
                std::memory_order_relaxed));  // 获得锁
            idx_new = item.size_;  // 获得当前边的相对插入位置
            if (item.capacity_ <= idx_new) {
              LOG(INFO) << "vertex_id: " << vertex_id << " " << item.capacity_
                        << " " << item.size_;
              assert(false);  // 没有空闲空间，需要重新分配
            }
            idx_new += item.start_idx_;  // 获得当前边的绝对插入位置
          },
          adj_list_item);
      // 插入边
      datas_of_all_column_family_[column_to_column_family.column_family_id]
          .csr[column_to_column_family.edge_list_id_in_column_family]
          ->set_single_obj(idx_new, value.second);

      gbp::BufferBlock::UpdateContent<MutableAdjlist>(
          [&](MutableAdjlist& item) {
            item.size_++;
            item.lock_.store(0);
          },
          adj_list_item);  // 释放锁
      break;
    }
    case gs::PropertyType::kEdge: {
      datas_of_all_column_family_[column_to_column_family.column_family_id]
          .fixed_length_column_family->setColumn(
              vertex_id, column_to_column_family.column_id_in_column_family,
              value.second);
      break;
    }
    default: {
      LOG(INFO) << "column_to_column_family.column_name: "
                << static_cast<int>(column_to_column_family.column_type);
      assert(false);
    }
    }
  }

  bool edge_label_with_direction_exist(size_t edge_label_id) {
    return edge_label_to_property_id_.find(edge_label_id) !=
           edge_label_to_property_id_.end();
  }

  std::map<size_t, size_t>& get_edge_label_to_property_id() {
    return edge_label_to_property_id_;
  }

  gbp::BufferBlock ReadEdges(size_t vertex_id, size_t edge_label_id,
                             size_t& edge_num) {
#if ASSERT_ENABLE
    assert(edge_label_to_property_id_.count(edge_label_id) == 1);
#endif

    auto column_to_column_family =
        property_id_to_ColumnToColumnFamily_configurations_
            [edge_label_to_property_id_[edge_label_id]];
    switch (column_to_column_family.column_type) {
    case PropertyType::kDynamicEdgeList: {
      auto item_t =
          datas_of_all_column_family_[column_to_column_family.column_family_id]
              .fixed_length_column_family->getColumn(
                  vertex_id,
                  column_to_column_family.column_id_in_column_family);
      auto& item = gbp::BufferBlock::Ref<MutableAdjlist>(item_t);
      edge_num = item.size_;
      return datas_of_all_column_family_[column_to_column_family
                                             .column_family_id]
          .csr[column_to_column_family.edge_list_id_in_column_family]
          ->get(item.start_idx_, item.size_);
    }
    case PropertyType::kEdge: {
      edge_num = 1;
      return ReadColumn(vertex_id, edge_label_to_property_id_[edge_label_id]);
    }
    default: {
      LOG(INFO) << edge_label_to_property_id_[edge_label_id] << " "
                << edge_label_id << " "
                << static_cast<int>(column_to_column_family.edge_type);
      assert(false);
      return gbp::BufferBlock();
    }
    }
  }

  // std::vector<std::pair<size_t, std::string_view>> void ReadVertex(
  //     size_t vertex_id) {
  //   for (auto& value : values) {
  //     InsertColumn(vertex_id, value);
  //   }
  // }

  gs::PropertyType GetColumnType(size_t property_id) {
#if ASSERT_ENABLE
    assert(property_id_to_ColumnToColumnFamily_configurations_.count(
               property_id) == 1);
#endif
    return property_id_to_ColumnToColumnFamily_configurations_[property_id]
        .column_type;
  }

  gbp::BufferBlock ReadColumn(size_t vertex_id, size_t property_id) {
#if ASSERT_ENABLE
    assert(property_id_to_ColumnToColumnFamily_configurations_.count(
               property_id) == 1);
#endif
    gbp::BufferBlock result;
    auto column_to_column_family =
        property_id_to_ColumnToColumnFamily_configurations_[property_id];
    switch (column_to_column_family.column_type) {
    case gs::PropertyType::kEdge:
    case gs::PropertyType::kInt32:
    case gs::PropertyType::kDate:
    case gs::PropertyType::kInt64: {
      result =
          datas_of_all_column_family_[column_to_column_family.column_family_id]
              .fixed_length_column_family->getColumn(
                  vertex_id,
                  column_to_column_family.column_id_in_column_family);
      break;
    }
    case gs::PropertyType::kString:
    case gs::PropertyType::kDynamicString: {
      // 存储string的position
      // string_item position = {start_pos, value.second.size()};
      auto position =
          datas_of_all_column_family_[column_to_column_family.column_family_id]
              .fixed_length_column_family->getColumn(
                  vertex_id,
                  column_to_column_family.column_id_in_column_family);
      auto& item = gbp::BufferBlock::Ref<string_item>(position);
      result =
          datas_of_all_column_family_[column_to_column_family.column_family_id]
              .stringpool->get(item.offset, item.length);
      break;
    }
    case gs::PropertyType::kDynamicEdgeList: {
      assert(false);
      break;
    }
    default: {
      assert(false);
    }
    }
    return result;
  }

  EdgeHandle getEdgeHandle(size_t edge_label_id) {
    // assert(edge_label_to_property_id_.count(edge_label_id) == 1);
    // assert(property_id_to_ColumnToColumnFamily_configurations_.count(edge_label_to_property_id_[edge_label_id])
    // == 1);
#if ASSERT_ENABLE
    assert(edge_label_to_property_id_.count(edge_label_id) == 1);
    assert(property_id_to_ColumnToColumnFamily_configurations_.count(
               edge_label_to_property_id_[edge_label_id]) == 1);
#endif
    auto column_to_column_family =
        property_id_to_ColumnToColumnFamily_configurations_
            [edge_label_to_property_id_[edge_label_id]];
    auto param1 =
        datas_of_all_column_family_[column_to_column_family.column_family_id]
            .fixed_length_column_family->getColumnHandle(
                column_to_column_family.column_id_in_column_family);
    gs::mmap_array_base* param2 = nullptr;
    if (column_to_column_family.column_type ==
        gs::PropertyType::kDynamicEdgeList) {
      param2 =
          datas_of_all_column_family_[column_to_column_family.column_family_id]
              .csr[column_to_column_family.edge_list_id_in_column_family];
      assert(param2 != nullptr);
    }
    auto param3 = column_to_column_family.column_type;
    return EdgeHandle(param1, param2, param3);
    // return EdgeHandle(
    //     datas_of_all_column_family_[column_to_column_family.column_family_id].fixed_length_column_family->getColumnHandle(column_to_column_family.column_id_in_column_family),
    //     datas_of_all_column_family_[column_to_column_family.column_family_id].csr[column_to_column_family.edge_list_id_in_column_family],
    //     column_to_column_family.edge_type);
  }

  PropertyHandle getPropertyHandle(size_t property_id) {
    auto column_to_column_family =
        property_id_to_ColumnToColumnFamily_configurations_[property_id];
    return PropertyHandle(
        datas_of_all_column_family_[column_to_column_family.column_family_id]
            .fixed_length_column_family->getColumnHandle(
                column_to_column_family.column_id_in_column_family),
        datas_of_all_column_family_[column_to_column_family.column_family_id]
            .stringpool,
        column_to_column_family.column_type);
  }

  void Resize(size_t new_capacity_in_row) {
    for (auto& data_per_column_family : datas_of_all_column_family_) {
      if (data_per_column_family.fixed_length_column_family == nullptr) {
        continue;
      }
      data_per_column_family.fixed_length_column_family->resize(
          new_capacity_in_row);
      if (!data_per_column_family.stringpool->read_only())
        data_per_column_family.stringpool->resize(new_capacity_in_row * 128 *
                                                  1);

      for (auto& csr : data_per_column_family.csr) {
        csr->resize(new_capacity_in_row * 128);
      }
    }
    for (auto& column_configuration :
         property_id_to_ColumnToColumnFamily_configurations_) {
      switch (column_configuration.second.column_type) {
      case gs::PropertyType::kDynamicEdgeList: {
        MutableAdjlist empty_adjlist;
        empty_adjlist.init(0, 0, 0);
        std::string_view empty_adjlist_view(
            reinterpret_cast<char*>(&empty_adjlist), sizeof(MutableAdjlist));
        for (size_t row_id = capacity_in_row_; row_id < new_capacity_in_row;
             row_id++) {
          datas_of_all_column_family_[column_configuration.second
                                          .column_family_id]
              .fixed_length_column_family->setColumn(
                  row_id,
                  column_configuration.second.column_id_in_column_family,
                  empty_adjlist_view);

          // auto item_t =
          //     datas_of_all_column_family_[column_configuration.second
          //                                     .column_family_id]
          //         .fixed_length_column_family->getColumn(
          //             row_id,
          //             column_configuration.second.column_id_in_column_family);
          // gbp::BufferBlock::UpdateContent<MutableAdjlist>(
          //     [&](MutableAdjlist& item) {
          //       item.capacity_ = 0;
          //       item.start_idx_ = 0;
          //       item.size_ = 0;
          //     },
          //     item_t);
        }
        break;
      }
      case gs::PropertyType::kEdge: {
        std::vector<char> empty_edge;
        assert(ConstructEdgeNew(empty_edge, std::string(), 0,
                                column_configuration.second.edge_type,
                                std::numeric_limits<timestamp_t>::max(), true));
        for (size_t row_id = capacity_in_row_; row_id < new_capacity_in_row;
             row_id++) {
          datas_of_all_column_family_[column_configuration.second
                                          .column_family_id]
              .fixed_length_column_family->setColumn(
                  row_id,
                  column_configuration.second.column_id_in_column_family,
                  std::string_view(empty_edge.data(), empty_edge.size()));
        }
        break;
      }
      }
    }
    capacity_in_row_ = new_capacity_in_row;
  }

  size_t CapacityInRow() const { return capacity_in_row_; }
  FixedLengthColumnFamily& GetColumnFamily(size_t column_family_id) {
    return *(datas_of_all_column_family_[column_family_id]
                 .fixed_length_column_family);
  }

  void Serialize(std::unique_ptr<grape::LocalIOAdaptor>& writer) {
    grape::InArchive arc;
    arc << property_id_to_ColumnToColumnFamily_configurations_
        << column_family_num_ << capacity_in_row_ << vertex_name_
        << column_family_info_ << edge_label_to_property_id_;
    CHECK(writer->WriteArchive(arc));
  }

  void Deserialize(std::unique_ptr<grape::LocalIOAdaptor>& reader) {
    grape::OutArchive arc;
    CHECK(reader->ReadArchive(arc));
    arc >> property_id_to_ColumnToColumnFamily_configurations_ >>
        column_family_num_ >> capacity_in_row_ >> vertex_name_ >>
        column_family_info_ >> edge_label_to_property_id_;
  }

  static bool ConstructEdgeNew(std::vector<char>& edge,
                               std::string_view e_property, size_t e_neighbor,
                               PropertyType e_property_type,
                               size_t timestamp = 0,
                               bool property_is_empty = false) {
    switch (e_property_type) {
    case PropertyType::kEmpty: {
      edge.resize(sizeof(MutableNbr<grape::EmptyType>));
      auto item = reinterpret_cast<MutableNbr<grape::EmptyType>*>(edge.data());
      item->neighbor = e_neighbor;
      item->timestamp = timestamp;
      break;
    }
    case PropertyType::kDate: {
      edge.resize(sizeof(MutableNbr<gs::Date>));
      auto item = reinterpret_cast<MutableNbr<gs::Date>*>(edge.data());
      item->neighbor = e_neighbor;
      item->timestamp = timestamp;
      if (!property_is_empty)
        item->data = *reinterpret_cast<const gs::Date*>(e_property.data());
      break;
    }
    case PropertyType::kInt32: {
      edge.resize(sizeof(MutableNbr<int32_t>));
      auto item = reinterpret_cast<MutableNbr<int32_t>*>(edge.data());
      item->neighbor = e_neighbor;
      item->timestamp = timestamp;
      if (!property_is_empty)
        item->data = *reinterpret_cast<const int32_t*>(e_property.data());
      break;
    }
    case PropertyType::kInt64: {
      edge.resize(sizeof(MutableNbr<int64_t>));
      auto item = reinterpret_cast<MutableNbr<int64_t>*>(edge.data());
      item->neighbor = e_neighbor;
      item->timestamp = timestamp;
      if (!property_is_empty)
        item->data = *reinterpret_cast<const int64_t*>(e_property.data());
      break;
    }
    case PropertyType::kDouble: {
      edge.resize(sizeof(MutableNbr<double>));
      auto item = reinterpret_cast<MutableNbr<double>*>(edge.data());
      item->neighbor = e_neighbor;
      item->timestamp = timestamp;
      if (!property_is_empty)
        item->data = *reinterpret_cast<const double*>(e_property.data());
      break;
    }
    default:
      assert(false);
    }

    return true;
  }

  std::string GetVertexName() const { return vertex_name_; }

 private:
  struct ColumnFamilyInfo {
    ColumnFamilyInfo() : column_number(0) {}
    ColumnFamilyInfo(size_t column_number) : column_number(column_number) {}
    ~ColumnFamilyInfo() = default;

    // 清晰的格式化输出
    friend grape::InArchive& operator<<(grape::InArchive& os,
                                        const ColumnFamilyInfo& obj) {
      // 使用适当的缩进和格式
      os << obj.column_number;
      os << obj.edge_list_sizes_;
      return os;
    }
    friend grape::OutArchive& operator>>(grape::OutArchive& os,
                                         ColumnFamilyInfo& obj) {
      os >> obj.column_number;
      os >> obj.edge_list_sizes_;
      return os;
    }

    size_t column_number;  // column number in fixed length column family
    // 必须要原子操作！！！
    std::vector<size_t>
        edge_list_sizes_;  // 每种边的文件中nbr光标，大于光标的空间是空闲空间，可随意分配，小于光标的空间是已分配空间，不可随意分配
  };

  size_t column_family_num_;
  std::vector<DataPerColumnFamily> datas_of_all_column_family_;
  std::vector<ColumnFamilyInfo> column_family_info_;

  std::map<size_t, ColumnConfiguration>
      property_id_to_ColumnToColumnFamily_configurations_;
  std::map<size_t, size_t> edge_label_to_property_id_;
  size_t capacity_in_row_ = 0;
  std::string db_dir_path_;
  std::string vertex_name_;
};
}  // namespace cgraph
}  // namespace gs