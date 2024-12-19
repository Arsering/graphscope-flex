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

#ifndef GRAPHSCOPE_DATABASE_READ_TRANSACTION_H_
#define GRAPHSCOPE_DATABASE_READ_TRANSACTION_H_

#include <array>
#include <limits>
#include <tuple>
#include <utility>
#include <vector>

#include "flex/storages/rt_mutable_graph/mutable_csr.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/storages/rt_mutable_graph/types.h"
#include "flex/utils/id_indexer.h"

namespace gs {

class MutablePropertyFragment;
class VersionManager;

#if OV
template <typename EDATA_T>
class AdjListView {
  class nbr_iterator {
    using nbr_t = MutableNbr<EDATA_T>;

   public:
    nbr_iterator(const nbr_t* ptr, const nbr_t* end, timestamp_t timestamp)
        : ptr_(ptr), end_(end), timestamp_(timestamp) {
      while (ptr_ != end && ptr_->timestamp > timestamp_) {
        ++ptr_;
      }
    }

    const nbr_t& operator*() const { return *ptr_; }

    const nbr_t* operator->() const { return ptr_; }

    nbr_iterator& operator++() {
      ++ptr_;
      while (ptr_ != end_ && ptr_->timestamp > timestamp_) {
        ++ptr_;
      }
      return *this;
    }

    bool operator==(const nbr_iterator& rhs) const {
      return (ptr_ == rhs.ptr_);
    }

    bool operator!=(const nbr_iterator& rhs) const {
      return (ptr_ != rhs.ptr_);
    }

   private:
    const nbr_t* ptr_;
    const nbr_t* end_;
    timestamp_t timestamp_;
  };

 public:
  using slice_t = MutableNbrSlice<EDATA_T>;

  AdjListView(const slice_t& slice, timestamp_t timestamp)
      : edges_(slice), timestamp_(timestamp) {}

  nbr_iterator begin() const {
    return nbr_iterator(edges_.begin(), edges_.end(), timestamp_);
  }
  nbr_iterator end() const {
    return nbr_iterator(edges_.end(), edges_.end(), timestamp_);
  }

  int estimated_degree() const { return edges_.size(); }

 private:
  slice_t edges_;
  timestamp_t timestamp_;
};
#else
template <typename EDATA_T>
class AdjListView {
 public:
  using slice_t = MutableNbrSlice<EDATA_T>;
  using sliceiter_t = TypedMutableCsrConstEdgeIter<EDATA_T>;

  AdjListView(const slice_t& slice, timestamp_t timestamp)
      : edges_(sliceiter_t(slice)), timestamp_(timestamp) {
    while (edges_.is_valid() && edges_.get_timestamp() > timestamp_) {
      edges_.next();
    }
  }

  FORCE_INLINE vid_t get_neighbor() { return edges_.get_neighbor(); }

  FORCE_INLINE const void* get_data() { return edges_.get_data(); }

  FORCE_INLINE timestamp_t get_timestamp() { return edges_.get_timestamp(); }

  FORCE_INLINE void next() {
    edges_.next();
    while (edges_.is_valid() && edges_.get_timestamp() > timestamp_) {
      edges_.next();
    }
  }

  FORCE_INLINE bool is_valid() {
    return edges_.is_valid() && edges_.get_timestamp() <= timestamp_;
  }

  int estimated_degree() const { return edges_.size(); }
  timestamp_t timestamp() const { return timestamp_; }

 private:
  sliceiter_t edges_;
  timestamp_t timestamp_;
};
#endif
template <typename EDATA_T>
class GraphView {
 public:
  GraphView(const MutableCsr<EDATA_T>& csr, timestamp_t timestamp)
      : csr_(csr), timestamp_(timestamp) {}

  AdjListView<EDATA_T> get_edges(vid_t v) const {
    return AdjListView<EDATA_T>(csr_.get_edges(v), timestamp_);
  }
  timestamp_t timestamp() const { return timestamp_; }

 private:
  const MutableCsr<EDATA_T>& csr_;
  timestamp_t timestamp_;
};

template <typename EDATA_T>
class SingleGraphView {
  using nbr_t = MutableNbr<EDATA_T>;

 public:
  SingleGraphView(const SingleMutableCsr<EDATA_T>& csr, timestamp_t timestamp)
      : csr_(csr), timestamp_(timestamp) {}
#if OV
  bool exist(vid_t v) const {
    return (csr_.get_edge(v).timestamp.load() <= timestamp_);
  }

  const MutableNbr<EDATA_T>& get_edge(vid_t v) const {
    return csr_.get_edge(v);
  }
#else
  FORCE_INLINE bool exist(vid_t v) const {
    auto item = csr_.get_edge(v);
    return (gbp::BufferBlock::Ref<nbr_t>(item).timestamp.load() <= timestamp_);
  }

  FORCE_INLINE const gbp::BufferBlock exist(vid_t v, bool& exist) const {
    auto item = csr_.get_edge(v);
    exist = gbp::BufferBlock::Ref<nbr_t>(item).timestamp.load() <= timestamp_;
    return item;
  }
  FORCE_INLINE bool exist1(gbp::BufferBlock& item) const {
    return gbp::BufferBlock::Ref<nbr_t>(item).timestamp.load() <= timestamp_;
  }
  FORCE_INLINE const gbp::BufferBlock get_edge(vid_t v) const {
    return csr_.get_edge(v);
  }

  FORCE_INLINE timestamp_t timestamp() const { return timestamp_; }
#endif
 private:
  const SingleMutableCsr<EDATA_T>& csr_;
  timestamp_t timestamp_;
};

class ReadTransaction {
 public:
  ReadTransaction(const MutablePropertyFragment& graph, VersionManager& vm,
                  timestamp_t timestamp);
  ~ReadTransaction();

  timestamp_t timestamp() const;

  void Commit();

  void Abort();

  class vertex_iterator {
   public:
    vertex_iterator(label_t label, vid_t cur, vid_t num,
                    const MutablePropertyFragment& graph);
    ~vertex_iterator();

    bool IsValid() const;
    void Next();
    void Goto(vid_t target);

    oid_t GetId() const;
    vid_t GetIndex() const;
#if OV
    Any GetField(int col_id) const;
#else
    gbp::BufferBlock GetField(int col_id) const;
#endif
    int FieldNum() const;

   private:
    label_t label_;
    vid_t cur_;
    vid_t num_;
    const MutablePropertyFragment& graph_;
  };

  class edge_iterator {
   public:
    edge_iterator(label_t neighbor_label, label_t edge_label,
                  std::shared_ptr<MutableCsrConstEdgeIterBase> iter);
    ~edge_iterator();
#if OV
    Any GetData() const;
#else
    const void* GetData() const;
#endif
    bool IsValid() const;

    void Next();

    vid_t GetNeighbor() const;

    label_t GetNeighborLabel() const;

    label_t GetEdgeLabel() const;

   private:
    label_t neighbor_label_;
    label_t edge_label_;

    std::shared_ptr<MutableCsrConstEdgeIterBase> iter_;
  };

  vertex_iterator GetVertexIterator(label_t label) const;

  vertex_iterator FindVertex(label_t label, oid_t id) const;

  bool GetVertexIndex(label_t label, oid_t id, vid_t& index) const;

  vid_t GetVertexNum(label_t label) const;

  oid_t GetVertexId(label_t label, vid_t index) const;

  edge_iterator GetOutEdgeIterator(label_t label, vid_t u,
                                   label_t neighnor_label,
                                   label_t edge_label) const;

  edge_iterator GetInEdgeIterator(label_t label, vid_t u,
                                  label_t neighnor_label,
                                  label_t edge_label) const;

  template <typename EDATA_T>
  AdjListView<EDATA_T> GetOutgoingEdges(label_t v_label, vid_t v,
                                        label_t neighbor_label,
                                        label_t edge_label) const {
    auto csr = dynamic_cast<const TypedMutableCsrBase<EDATA_T>*>(
        graph_.get_oe_csr(v_label, neighbor_label, edge_label));

    return AdjListView<EDATA_T>(csr->get_edges(v), timestamp_);
  }

  template <typename EDATA_T>
  AdjListView<EDATA_T> GetIncomingEdges(label_t v_label, vid_t v,
                                        label_t neighbor_label,
                                        label_t edge_label) const {
    auto csr = dynamic_cast<const TypedMutableCsrBase<EDATA_T>*>(
        graph_.get_ie_csr(v_label, neighbor_label, edge_label));
    return AdjListView<EDATA_T>(csr->get_edges(v), timestamp_);
  }

  const Schema& schema() const;

  template <typename EDATA_T>
  GraphView<EDATA_T> GetOutgoingGraphView(label_t v_label,
                                          label_t neighbor_label,
                                          label_t edge_label) const {
    auto csr = dynamic_cast<const MutableCsr<EDATA_T>*>(
        graph_.get_oe_csr(v_label, neighbor_label, edge_label));
    return GraphView<EDATA_T>(*csr, timestamp_);
  }

  template <typename EDATA_T>
  GraphView<EDATA_T> GetIncomingGraphView(label_t v_label,
                                          label_t neighbor_label,
                                          label_t edge_label) const {
    auto csr = dynamic_cast<const MutableCsr<EDATA_T>*>(
        graph_.get_ie_csr(v_label, neighbor_label, edge_label));
    return GraphView<EDATA_T>(*csr, timestamp_);
  }

  template <typename EDATA_T>
  SingleGraphView<EDATA_T> GetOutgoingSingleGraphView(
      label_t v_label, label_t neighbor_label, label_t edge_label) const {
    auto csr = dynamic_cast<const SingleMutableCsr<EDATA_T>*>(
        graph_.get_oe_csr(v_label, neighbor_label, edge_label));
    return SingleGraphView<EDATA_T>(*csr, timestamp_);
  }

  template <typename EDATA_T>
  SingleGraphView<EDATA_T> GetIncomingSingleGraphView(
      label_t v_label, label_t neighbor_label, label_t edge_label) const {
    auto csr = dynamic_cast<const SingleMutableCsr<EDATA_T>*>(
        graph_.get_ie_csr(v_label, neighbor_label, edge_label));
    return SingleGraphView<EDATA_T>(*csr, timestamp_);
  }

  /*
  批量获取顶点ID
  @param label: 顶点标签
  @param indices: 顶点索引组成的vector
  @return: 顶点ID组成的vector
  */
  std::vector<oid_t> BatchGetVertexIds(label_t label,
                                       const std::vector<vid_t>& indices) const;

  /*
  批量查找顶点索引
  @param label: 顶点标签
  @param oids: 顶点ID组成的vector
  @return: 顶点索引组成的vector和是否存在的vector
  */
  std::pair<std::vector<vid_t>, std::vector<bool>> BatchGetVertexIndices(
      label_t label, const std::vector<oid_t>& oids) const;

  /*
  批量获取顶点属性
  @param label: 顶点标签
  @param vids: 顶点ID组成的vector
  @param col_ids: 属性ID组成的vector
  @return: 顶点属性组成的vector
  */
  // template <typename... T>
  // std::vector<std::tuple<T...>> BatchGetVertexPropsFromVid(
  //     label_t label, const std::vector<vid_t>& vids,
  //     const std::array<int, sizeof...(T)>& col_ids) const {
  //   std::vector<std::tuple<T...>> results(vids.size());
  //   auto& table = graph_.get_vertex_table(label);
    
  //   // 获取所有列
  //   std::array<const ColumnBase*, sizeof...(T)> columns;
  //   for(size_t i = 0; i < sizeof...(T); i++) {
  //     columns[i] = table.get_column_by_id(col_ids[i]);
  //     CHECK(columns[i] != nullptr) << "Column " << col_ids[i] << " not found";
  //   }

  //   // 为每个顶点获取属性值
  //   for(size_t i = 0; i < vids.size(); i++) {
  //     auto vid = vids[i];
  //     results[i] = std::make_tuple(
  //       std::dynamic_pointer_cast<TypedColumn<T>>(columns[std::index_sequence_for<T...>{}])->get_view(vid)...
  //     );
  //   }
  //   return results;
  // }

  /*
  批量获取顶点属性
  @param label_id: 顶点标签
  @param vids: 顶点ID组成的vector
  @param prop_names: 属性名称组成的vector
  @return: 顶点属性组成的vector
  */
  std::vector<gbp::BufferBlock> BatchGetVertexPropsFromVid(
      label_t label_id, const std::vector<vid_t>& vids,
      const std::vector<std::string>& prop_names) const {
    std::vector<gbp::BufferBlock> results;
    results.reserve(vids.size() * prop_names.size());  // 预分配空间
    auto& table = graph_.get_vertex_table(label_id);
    
    // 获取所有列
    std::vector<std::shared_ptr<ColumnBase>> columns;
    columns.reserve(prop_names.size());
    for(const auto& prop_name : prop_names) {
      auto column = table.get_column(prop_name);
      CHECK(column != nullptr) << "Column " << prop_name << " not found";
      columns.push_back(column);
    }

    // 为每个顶点获取属性值
    for(size_t i = 0; i < vids.size(); i++) {
      auto vid = vids[i];
      for(const auto& column : columns) {
        auto type = column->type();
        if(type == PropertyType::kString) {
          auto string_column = std::dynamic_pointer_cast<StringColumn>(column);
          CHECK(string_column != nullptr);
          results.push_back(string_column->get(vid));
        } else if(type == PropertyType::kDate) {
          auto date_column = std::dynamic_pointer_cast<TypedColumn<Date>>(column);
          CHECK(date_column != nullptr);
          results.push_back(date_column->get(vid));
        } else if(type == PropertyType::kInt32) {
          auto int_column = std::dynamic_pointer_cast<TypedColumn<int>>(column);
          CHECK(int_column != nullptr);
          results.push_back(int_column->get(vid));
        } else if(type == PropertyType::kInt64) {
          auto int64_column = std::dynamic_pointer_cast<TypedColumn<int64_t>>(column);
          CHECK(int64_column != nullptr);
          results.push_back(int64_column->get(vid));
        } else {
          LOG(FATAL) << "Unsupported property type: " << static_cast<int>(type);
        }
      }
    }
    return results;
  }

  /* 批量获取出边
  @param v_label: 顶点标签             
  @param vids: 顶点ID组成的vector
  @param neighbor_label: 邻居标签
  @param edge_label: 边标签
  @return: 出边列表
  */
  template <typename EDATA_T>
  std::vector<AdjListView<EDATA_T>> BatchGetOutgoingEdges(
      label_t v_label, const std::vector<vid_t>& vids, label_t neighbor_label,
      label_t edge_label) const;

  /* 批量获取入边
  @param v_label: 顶点标签
  @param vids: 顶点ID组成的vector
  @param neighbor_label: 邻居标签
  @param edge_label: 边标签
  @return: 入边列表
  */
  template <typename EDATA_T>
  std::vector<AdjListView<EDATA_T>> BatchGetIncomingEdges(
      label_t v_label, const std::vector<vid_t>& vids, label_t neighbor_label,
      label_t edge_label) const;

  /* 获取边指向的邻居
  @param src_label_id: 源顶点标签
  @param dst_label_id: 目标顶点标签
  @param edge_label_id: 边标签
  @param vids: 顶点ID组成的vector
  @param direction_str: 方向
  @return: 邻居列表
  */
  std::vector<std::vector<vid_t>> GetOtherVertices(
      const label_t& src_label_id, const label_t& dst_label_id,
      const label_t& edge_label_id, const std::vector<vid_t>& vids,
      const std::string& direction_str) const;


  template <typename FUNC_T, typename... SELECTOR>
  void ScanVertices(const std::string& label,
                    const std::tuple<SELECTOR...>& props,
                    const FUNC_T& func) const {
    auto label_id = graph_.schema().get_vertex_label_id(label);
    auto vnum = graph_.vertex_num(label_id);

    // 获取属性列
    auto columns = get_tuple_column_from_graph_with_property(label_id, props);
    std::tuple<typename SELECTOR::prop_t...> t;

    // 遍历所有顶点
    if constexpr (sizeof...(SELECTOR) == 0) {
      // 无属性的情况
      for (auto v = 0; v != vnum; ++v) {
        func(v, t);
      }
    } else {
      // 有属性的情况
      for (auto v = 0; v != vnum; ++v) {
        get_tuple_from_column_tuple(v, t, columns);
        func(v, t);
      }
    }
  }

 private:
  void release();

  const MutablePropertyFragment& graph_;
  VersionManager& vm_;
  timestamp_t timestamp_;

  template <size_t Is = 0, typename... T, typename column_tuple_t>
  void fetch_properties_in_column(const std::vector<vid_t>& vids,
                                  std::vector<std::tuple<T...>>& props,
                                  column_tuple_t& column) const {
    // auto index_seq = std::make_index_sequence<sizeof...(T)>{};

    auto& cur_column = std::get<Is>(column);
    if (cur_column) {
      for (auto i = 0; i < vids.size(); ++i) {
        std::get<Is>(props[i]) = cur_column->get_view(vids[i]);
      }
    }

    if constexpr (Is + 1 < sizeof...(T)) {
      fetch_properties_in_column<Is + 1>(vids, props, column);
    }
  }

  // template<typename... T, size_t... Is>
  // void get_buffers_helper(
  //     const std::array<std::shared_ptr<ColumnBase>, sizeof...(T)>& columns,
  //     vid_t vid,
  //     std::index_sequence<Is...>,
  //     std::vector<gbp::BufferBlock>& results) const {
  //   using expander = int[];
  //   (void)expander{0,
  //     ((void)(results.push_back(
  //       [&]() -> gbp::BufferBlock {
  //         using Type = std::tuple_element_t<Is, std::tuple<T...>>;
  //         if constexpr (std::is_same_v<Type, std::string_view>) {
  //           auto column = std::dynamic_pointer_cast<StringColumn>(columns[Is]);
  //           CHECK(column != nullptr);
  //           return column->get(vid);
  //         } else {
  //           auto column = std::dynamic_pointer_cast<TypedColumn<Type>>(columns[Is]);
  //           CHECK(column != nullptr);
  //           return column->get(vid);
  //         }
  //       }()
  //     )), 0)...
  //   };
  // }
};

}  // namespace gs

#endif  // GRAPHSCOPE_DATABASE_READ_TRANSACTION_H_
