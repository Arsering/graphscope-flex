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

#ifndef GRAPHSCOPE_FRAGMENT_MUTABLE_PROPERTY_FRAGMENT_H_
#define GRAPHSCOPE_FRAGMENT_MUTABLE_PROPERTY_FRAGMENT_H_

#include <cstddef>
#include <thread>
#include <tuple>
#include <vector>

#include "flex/storages/rt_mutable_graph/schema.h"

#include "flex/storages/rt_mutable_graph/mutable_csr.h"
#include "flex/storages/rt_mutable_graph/types.h"
#include "flex/storages/rt_mutable_graph/vertex.h"
#include "flex/utils/arrow_utils.h"
#include "flex/utils/id_indexer.h"
#include "flex/utils/property/table.h"
#include "flex/utils/property/types.h"
#include "flex/utils/yaml_utils.h"
#include "grape/io/local_io_adaptor.h"
#include "grape/serialization/out_archive.h"

namespace gs {

class MutablePropertyFragment {
 public:
  MutablePropertyFragment();

  ~MutablePropertyFragment();

  void IngestEdge(label_t src_label, vid_t src_lid, label_t dst_label,
                  vid_t dst_lid, label_t edge_label, timestamp_t ts,
                  grape::OutArchive& arc, MMapAllocator& alloc);

  void Open(const std::string& work_dir);
  void Dump(const std::string& work_dir, uint32_t version);
  void DumpSchema(const std::string& filename);

  const Schema& schema() const;

  Table& get_vertex_table(label_t vertex_label);

  const Table& get_vertex_table(label_t vertex_label) const;

  vid_t vertex_num(label_t vertex_label) const;

  bool get_lid(label_t label, oid_t oid, vid_t& lid) const;

  oid_t get_oid(label_t label, vid_t lid) const;

  vid_t add_vertex(label_t label, oid_t id);
  vid_t add_vertex(label_t label, oid_t id, label_t creator_label,
                   oid_t creator_id);
  std::shared_ptr<MutableCsrConstEdgeIterBase> get_outgoing_edges(
      label_t label, vid_t u, label_t neighbor_label, label_t edge_label) const;

  std::shared_ptr<MutableCsrConstEdgeIterBase> get_incoming_edges(
      label_t label, vid_t u, label_t neighbor_label, label_t edge_label) const;

  std::shared_ptr<MutableCsrEdgeIterBase> get_outgoing_edges_mut(
      label_t label, vid_t u, label_t neighbor_label, label_t edge_label);

  std::shared_ptr<MutableCsrEdgeIterBase> get_incoming_edges_mut(
      label_t label, vid_t u, label_t neighbor_label, label_t edge_label);

  MutableCsrConstEdgeIterBase* get_outgoing_edges_raw(label_t label, vid_t u,
                                                      label_t neighbor_label,
                                                      label_t edge_label) const;

  MutableCsrConstEdgeIterBase* get_incoming_edges_raw(label_t label, vid_t u,
                                                      label_t neighbor_label,
                                                      label_t edge_label) const;

  MutableCsrBase* get_oe_csr(label_t label, label_t neighbor_label,
                             label_t edge_label);

  const MutableCsrBase* get_oe_csr(label_t label, label_t neighbor_label,
                                   label_t edge_label) const;

  MutableCsrBase* get_ie_csr(label_t label, label_t neighbor_label,
                             label_t edge_label);

  const MutableCsrBase* get_ie_csr(label_t label, label_t neighbor_label,
                                   label_t edge_label) const;

  void loadSchema(const std::string& filename);
  void cgraph_open(const std::string& snapshot_dir_path);

  cgraph::Vertex& get_vertices(label_t label) { return vertices_[label]; }
  void ingest_vertex(label_t label, vid_t vid, grape::OutArchive& arc);
  void ingest_edge(label_t src_label, vid_t src_lid, label_t dst_label,
                   vid_t dst_lid, label_t edge_label, timestamp_t ts,
                   grape::OutArchive& arc);
  void test_dynamic_buffered_indexer(size_t vertex_id);
  void test_vertex_copy_edge_list(size_t vertex_id);
  void copy_vertex_column_data(size_t vertex_id,size_t property_id, cgraph::PropertyHandle& property_handle, size_t old_vid, size_t new_vid);
  void copy_vertex_single_edge(size_t vertex_id,size_t edge_label_id,gs::PropertyType property_type, cgraph::EdgeHandle& edge_handle, size_t old_vid, size_t new_vid);
  void copy_vertex_dynamic_edge(size_t vertex_id,size_t edge_label_id,size_t old_vid, size_t new_vid,
                                               size_t column_family_id,
                                               size_t column_id_in_column_family);
  void range_copy_vertex_data(size_t vertex_label_id, std::vector<size_t> vertex_ids, std::map<size_t, size_t>* old_index_to_new_index);
  void batch_reorder_indexer(size_t vertex_id);
  void test_vertex_copy();
  void test_edge_list_change();
  void change_edge_list_layout(size_t src_vertex_label,size_t dst_vertex_label,size_t edge_label_id,bool is_outgoing,size_t left, size_t right,std::map<size_t, size_t>* old_index_to_new_index);
  void check_copy_result(size_t vertex_label_id,std::vector<size_t> vertex_ids,std::map<size_t, size_t>* old_index_to_new_index);

  Schema schema_;
  std::vector<LFIndexer<vid_t>> lf_indexers_;
  std::vector<MutableCsrBase*> ie_, oe_;
  std::vector<Table> vertex_data_;

  size_t vertex_label_num_, edge_label_num_;

  std::vector<cgraph::Vertex> vertices_;
  std::vector<BaseIndexer<vid_t>*> cgraph_lf_indexers_;
};

}  // namespace gs

#endif  // GRAPHSCOPE_FRAGMENT_MUTABLE_PROPERTY_FRAGMENT_H_