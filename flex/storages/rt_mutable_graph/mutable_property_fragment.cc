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

#include "grape/util.h"

#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"

#include "flex/engines/hqps_db/core/utils/hqps_utils.h"
#include "flex/graphscope_bufferpool/include/buffer_pool_manager.h"
#include "flex/storages/rt_mutable_graph/file_names.h"
#include "flex/utils/property/types.h"

namespace gs {

MutablePropertyFragment::MutablePropertyFragment() {}

MutablePropertyFragment::~MutablePropertyFragment() {
  std::vector<size_t> degree_list(vertex_label_num_, 0);
  for (size_t i = 0; i < vertex_label_num_; ++i) {
    // if(i==schema_.get_comment_label_id()){
    //   degree_list[i] = message_id_allocator_.size();
    // }else{
    //   degree_list[i] = lf_indexers_[i].size();
    // }
    degree_list[i] = base_indexers_[i]->size();
    vertex_data_[i].resize(degree_list[i]);
  }
  for (size_t src_label = 0; src_label != vertex_label_num_; ++src_label) {
    for (size_t dst_label = 0; dst_label != vertex_label_num_; ++dst_label) {
      for (size_t e_label = 0; e_label != edge_label_num_; ++e_label) {
        size_t index = src_label * vertex_label_num_ * edge_label_num_ +
                       dst_label * edge_label_num_ + e_label;
        if (ie_[index] != NULL) {
          ie_[index]->resize(degree_list[dst_label]);
          delete ie_[index];
        }
        if (oe_[index] != NULL) {
          oe_[index]->resize(degree_list[src_label]);
          delete oe_[index];
        }
      }
    }
  }
}

void MutablePropertyFragment::loadSchema(const std::string& schema_path) {
  auto io_adaptor = std::unique_ptr<grape::LocalIOAdaptor>(
      new grape::LocalIOAdaptor(schema_path));
  io_adaptor->Open();
  schema_.Deserialize(io_adaptor);
}

void MutablePropertyFragment::DumpSchema(const std::string& schema_path) {
  auto io_adaptor = std::unique_ptr<grape::LocalIOAdaptor>(
      new grape::LocalIOAdaptor(schema_path));
  io_adaptor->Open("wb");
  schema_.Serialize(io_adaptor);
  io_adaptor->Close();
}

inline MutableCsrBase* create_csr(EdgeStrategy es,
                                  const std::vector<PropertyType>& properties) {
  if (properties.empty()) {
    if (es == EdgeStrategy::kSingle) {
      return new SingleMutableCsr<grape::EmptyType>();
    } else if (es == EdgeStrategy::kMultiple) {
      return new MutableCsr<grape::EmptyType>();
    } else if (es == EdgeStrategy::kNone) {
      return new EmptyCsr<grape::EmptyType>();
    }
  } else if (properties[0] == PropertyType::kInt32) {
    if (es == EdgeStrategy::kSingle) {
      return new SingleMutableCsr<int>();
    } else if (es == EdgeStrategy::kMultiple) {
      return new MutableCsr<int>();
    } else if (es == EdgeStrategy::kNone) {
      return new EmptyCsr<int>();
    }
  } else if (properties[0] == PropertyType::kDate) {
    if (es == EdgeStrategy::kSingle) {
      return new SingleMutableCsr<Date>();
    } else if (es == EdgeStrategy::kMultiple) {
      return new MutableCsr<Date>();
    } else if (es == EdgeStrategy::kNone) {
      return new EmptyCsr<Date>();
    }
  } else if (properties[0] == PropertyType::kInt64) {
    if (es == EdgeStrategy::kSingle) {
      return new SingleMutableCsr<int64_t>();
    } else if (es == EdgeStrategy::kMultiple) {
      return new MutableCsr<int64_t>();
    } else if (es == EdgeStrategy::kNone) {
      return new EmptyCsr<int64_t>();
    }
  } else if (properties[0] == PropertyType::kDouble) {
    if (es == EdgeStrategy::kSingle) {
      return new SingleMutableCsr<double>();
    } else if (es == EdgeStrategy::kMultiple) {
      return new MutableCsr<double>();
    } else if (es == EdgeStrategy::kNone) {
      return new EmptyCsr<double>();
    }
  }
  LOG(FATAL) << "not support edge strategy or edge data type";
  return nullptr;
}

std::string get_latest_snapshot(const std::string& work_dir) {
  std::string snapshots_dir = work_dir + "/snapshots";
  uint32_t version;
  {
    FILE* fin = fopen((snapshots_dir + "/VERSION").c_str(), "r");
    auto ret = ::fread(&version, sizeof(uint32_t), 1, fin);
  }
  return snapshots_dir + "/" + std::to_string(version);
}

void MutablePropertyFragment::Open(const std::string& work_dir) {
  loadSchema(schema_path(work_dir));
  vertex_label_num_ = schema_.vertex_label_num();
  edge_label_num_ = schema_.edge_label_num();

  // lf_indexers_.resize(vertex_label_num_);
  base_indexers_.resize(vertex_label_num_);
  for(size_t i=0;i<vertex_label_num_;i++){
    if(i!=schema_.get_comment_label_id()&&i!=schema_.get_post_label_id()){
      base_indexers_[i]=new LFIndexer<vid_t>();
    }else{
      base_indexers_[i]=new MessageIdAllocator<oid_t, vid_t>();
    }
  }
  vertex_data_.resize(vertex_label_num_);
  std::string snapshot_dir = get_latest_snapshot(work_dir);
  std::string tmp_dir_path = tmp_dir(work_dir);
  std::filesystem::create_directory(tmp_dir_path);
  std::vector<size_t> vertex_capacities(vertex_label_num_, 0);

  auto t0 = -grape::GetCurrentTime();
  size_t size_in_byte_lf_indexer = 0;
  size_t size_in_byte_vertex_data = 0;
  for (size_t i = 0; i < vertex_label_num_; ++i) {
    std::string v_label_name = schema_.get_vertex_label_name(i);
    // if(i==schema_.get_comment_label_id()){
    //   message_id_allocator_.open(vertex_map_prefix(v_label_name)+".msg_allocator", snapshot_dir, 
    //                              tmp_dir_path);
    // }else{
    //   lf_indexers_[i].open(vertex_map_prefix(v_label_name), snapshot_dir,
    //                      tmp_dir_path);
    // }
    if(i==schema_.get_comment_label_id()||i==schema_.get_post_label_id()){
      dynamic_cast<MessageIdAllocator<oid_t, vid_t>*>(base_indexers_[i])->open(vertex_map_prefix(v_label_name)+".msg_allocator", snapshot_dir,
                              tmp_dir_path);
    }else{
      base_indexers_[i]->open(vertex_map_prefix(v_label_name), snapshot_dir,
                      tmp_dir_path);
    }
    // size_in_byte_lf_indexer += lf_indexers_[i].get_size_in_byte();
    vertex_data_[i].open(vertex_table_prefix(v_label_name), snapshot_dir,
                         tmp_dir_path, schema_.get_vertex_property_names(i),
                         schema_.get_vertex_properties(i),
                         schema_.get_vertex_storage_strategies(v_label_name));

    size_in_byte_vertex_data += vertex_data_[i].get_size_in_byte();

    size_t vertex_num;
    // if(i==schema_.get_comment_label_id()){
    //   vertex_num = message_id_allocator_.size();
    // }else{
    //   vertex_num = lf_indexers_[i].size();
    // }
    vertex_num = base_indexers_[i]->size();
    size_t vertex_capacity = vertex_num;
    // TODO:
    vertex_capacity += vertex_capacity >> 2;
    vertex_data_[i].resize(vertex_capacity);
    vertex_capacities[i] = vertex_capacity;
  }
  t0 += grape::GetCurrentTime();
  LOG(INFO) << "Time used = " << t0;
  size_t MB_in_byte = 1024LU * 1024;
  // LOG(INFO) << "size_in_MB_lf_indexer=" << size_in_byte_lf_indexer / MB_in_byte
  //           << " | size_in_MB_vertex_data="
  //           << size_in_byte_vertex_data / MB_in_byte;

  ie_.resize(vertex_label_num_ * vertex_label_num_ * edge_label_num_, NULL);
  oe_.resize(vertex_label_num_ * vertex_label_num_ * edge_label_num_, NULL);

  t0 = -grape::GetCurrentTime();
  size_t size_in_byte_edge_index = 0;
  size_t size_in_byte_edge_data = 0;
  for (size_t src_label_i = 0; src_label_i != vertex_label_num_;
       ++src_label_i) {
    std::string src_label =
        schema_.get_vertex_label_name(static_cast<label_t>(src_label_i));
    for (size_t dst_label_i = 0; dst_label_i != vertex_label_num_;
         ++dst_label_i) {
      std::string dst_label =
          schema_.get_vertex_label_name(static_cast<label_t>(dst_label_i));
      for (size_t e_label_i = 0; e_label_i != edge_label_num_; ++e_label_i) {
        std::string edge_label =
            schema_.get_edge_label_name(static_cast<label_t>(e_label_i));
        if (!schema_.exist(src_label, dst_label, edge_label)) {
          continue;
        }
        size_t index = src_label_i * vertex_label_num_ * edge_label_num_ +
                       dst_label_i * edge_label_num_ + e_label_i;
        auto& properties =
            schema_.get_edge_properties(src_label, dst_label, edge_label);
        EdgeStrategy oe_strategy = schema_.get_outgoing_edge_strategy(
            src_label, dst_label, edge_label);
        EdgeStrategy ie_strategy = schema_.get_incoming_edge_strategy(
            src_label, dst_label, edge_label);
        ie_[index] = create_csr(ie_strategy, properties);
        oe_[index] = create_csr(oe_strategy, properties);
        ie_[index]->open(ie_prefix(src_label, dst_label, edge_label),
                         snapshot_dir, tmp_dir_path);
        ie_[index]->resize(vertex_capacities[dst_label_i]);
        oe_[index]->open(oe_prefix(src_label, dst_label, edge_label),
                         snapshot_dir, tmp_dir_path);

        oe_[index]->resize(vertex_capacities[src_label_i]);
        size_in_byte_edge_index += ie_[index]->get_index_size_in_byte();
        size_in_byte_edge_index += oe_[index]->get_index_size_in_byte();
        size_in_byte_edge_data += ie_[index]->get_data_size_in_byte();
        size_in_byte_edge_data += oe_[index]->get_data_size_in_byte();
      }
    }
  }
  t0 += grape::GetCurrentTime();
  LOG(INFO) << "Time used = " << t0;
  LOG(INFO) << "size_in_MB_edge_index=" << size_in_byte_edge_index / MB_in_byte
            << " | size_in_MB_edge_data="
            << size_in_byte_edge_data / MB_in_byte;
}

void MutablePropertyFragment::Dump(const std::string& work_dir,
                                   uint32_t version) {
  std::string snapshot_dir_path = snapshot_dir(work_dir, version);
  std::filesystem::create_directories(snapshot_dir_path);
  std::vector<size_t> vertex_num(vertex_label_num_, 0);
  for (size_t i = 0; i < vertex_label_num_; ++i) {
    // if(i==schema_.get_comment_label_id()){
    //   vertex_num[i] = message_id_allocator_.size();
    // }else{
    //   vertex_num[i] = lf_indexers_[i].size();
    // }
    vertex_num[i] = base_indexers_[i]->size();
    // if(i==schema_.get_comment_label_id()){
    //   message_id_allocator_.dump(vertex_map_prefix(schema_.get_vertex_label_name(i)),
    //                      snapshot_dir_path);
    // }else{
    //   lf_indexers_[i].dump(vertex_map_prefix(schema_.get_vertex_label_name(i)),
    //                      snapshot_dir_path);
    // }
    base_indexers_[i]->dump(vertex_map_prefix(schema_.get_vertex_label_name(i)),
                         snapshot_dir_path);
    vertex_data_[i].resize(vertex_num[i]);
    vertex_data_[i].dump(vertex_table_prefix(schema_.get_vertex_label_name(i)),
                         snapshot_dir_path);
  }

  for (size_t src_label_i = 0; src_label_i != vertex_label_num_;
       ++src_label_i) {
    std::string src_label =
        schema_.get_vertex_label_name(static_cast<label_t>(src_label_i));
    for (size_t dst_label_i = 0; dst_label_i != vertex_label_num_;
         ++dst_label_i) {
      std::string dst_label =
          schema_.get_vertex_label_name(static_cast<label_t>(dst_label_i));
      for (size_t e_label_i = 0; e_label_i != edge_label_num_; ++e_label_i) {
        std::string edge_label =
            schema_.get_edge_label_name(static_cast<label_t>(e_label_i));
        if (!schema_.exist(src_label, dst_label, edge_label)) {
          continue;
        }
        size_t index = src_label_i * vertex_label_num_ * edge_label_num_ +
                       dst_label_i * edge_label_num_ + e_label_i;
        if (ie_[index] != NULL) {
          ie_[index]->resize(vertex_num[dst_label_i]);
          ie_[index]->dump(ie_prefix(src_label, dst_label, edge_label),
                           snapshot_dir_path);
        }
        if (oe_[index] != NULL) {
          oe_[index]->resize(vertex_num[src_label_i]);
          oe_[index]->dump(oe_prefix(src_label, dst_label, edge_label),
                           snapshot_dir_path);
        }
      }
    }
  }
  set_snapshot_version(work_dir, version);
}

void MutablePropertyFragment::IngestEdge(label_t src_label, vid_t src_lid,
                                         label_t dst_label, vid_t dst_lid,
                                         label_t edge_label, timestamp_t ts,
                                         grape::OutArchive& arc,
                                         MMapAllocator& alloc) {
  size_t index = src_label * vertex_label_num_ * edge_label_num_ +
                 dst_label * edge_label_num_ + edge_label;
  ie_[index]->peek_ingest_edge(dst_lid, src_lid, arc, ts, alloc);
  oe_[index]->ingest_edge(src_lid, dst_lid, arc, ts, alloc);
}

const Schema& MutablePropertyFragment::schema() const { return schema_; }

Table& MutablePropertyFragment::get_vertex_table(label_t vertex_label) {
  return vertex_data_[vertex_label];
}

const Table& MutablePropertyFragment::get_vertex_table(
    label_t vertex_label) const {
  return vertex_data_[vertex_label];
}

vid_t MutablePropertyFragment::vertex_num(label_t vertex_label) const {
  // if(vertex_label==schema_.get_comment_label_id()){
  //   return message_id_allocator_.size();
  // }else{
  //   return static_cast<vid_t>(lf_indexers_[vertex_label].size());
  // }
  return static_cast<vid_t>(base_indexers_[vertex_label]->size());
}

bool MutablePropertyFragment::get_lid(label_t label, oid_t oid,
                                      vid_t& lid) const {
  // if(label!=schema_.get_comment_label_id()){
  //   return lf_indexers_[label].get_index(oid, lid);
  // }else{
  //   return message_id_allocator_.get_lid_by_oid(oid, lid);
  // }
  return base_indexers_[label]->get_index(oid, lid);
}

oid_t MutablePropertyFragment::get_oid(label_t label, vid_t lid) const {
  // if(label!=schema_.get_comment_label_id()){
  //   return lf_indexers_[label].get_key(lid);
  // }else{
  //   oid_t oid;
  //   if(message_id_allocator_.get_oid_by_lid(lid, oid)){
  //     return oid;
  //   }
  // }
  return base_indexers_[label]->get_key(lid);
  assert(false);
}

vid_t MutablePropertyFragment::add_vertex(label_t label, oid_t id) {
  return lf_indexers_[label].insert(id);
}

vid_t MutablePropertyFragment::add_vertex(label_t label, oid_t id, label_t creator_label, oid_t creator_id) {
  vid_t creator_lid;
  get_lid(creator_label, creator_id, creator_lid);
  // return message_id_allocator_.get_message_id(creator_lid, id);
  return dynamic_cast<MessageIdAllocator<oid_t, vid_t>*>(base_indexers_[label])->get_message_id(creator_lid, id);
}

std::shared_ptr<MutableCsrConstEdgeIterBase>
MutablePropertyFragment::get_outgoing_edges(label_t label, vid_t u,
                                            label_t neighbor_label,
                                            label_t edge_label) const {
  size_t index = label * vertex_label_num_ * edge_label_num_ +
                 neighbor_label * edge_label_num_ + edge_label;
  return oe_[index]->edge_iter(u);
}

std::shared_ptr<MutableCsrConstEdgeIterBase>
MutablePropertyFragment::get_incoming_edges(label_t label, vid_t u,
                                            label_t neighbor_label,
                                            label_t edge_label) const {
  size_t index = neighbor_label * vertex_label_num_ * edge_label_num_ +
                 label * edge_label_num_ + edge_label;
  return ie_[index]->edge_iter(u);
}

MutableCsrConstEdgeIterBase* MutablePropertyFragment::get_outgoing_edges_raw(
    label_t label, vid_t u, label_t neighbor_label, label_t edge_label) const {
  size_t index = label * vertex_label_num_ * edge_label_num_ +
                 neighbor_label * edge_label_num_ + edge_label;
  return oe_[index]->edge_iter_raw(u);
}

MutableCsrConstEdgeIterBase* MutablePropertyFragment::get_incoming_edges_raw(
    label_t label, vid_t u, label_t neighbor_label, label_t edge_label) const {
  size_t index = neighbor_label * vertex_label_num_ * edge_label_num_ +
                 label * edge_label_num_ + edge_label;
  return ie_[index]->edge_iter_raw(u);
}

std::shared_ptr<MutableCsrEdgeIterBase>
MutablePropertyFragment::get_outgoing_edges_mut(label_t label, vid_t u,
                                                label_t neighbor_label,
                                                label_t edge_label) {
  size_t index = label * vertex_label_num_ * edge_label_num_ +
                 neighbor_label * edge_label_num_ + edge_label;
  return oe_[index]->edge_iter_mut(u);
}

std::shared_ptr<MutableCsrEdgeIterBase>
MutablePropertyFragment::get_incoming_edges_mut(label_t label, vid_t u,
                                                label_t neighbor_label,
                                                label_t edge_label) {
  size_t index = neighbor_label * vertex_label_num_ * edge_label_num_ +
                 label * edge_label_num_ + edge_label;
  return ie_[index]->edge_iter_mut(u);
}

MutableCsrBase* MutablePropertyFragment::get_oe_csr(label_t label,
                                                    label_t neighbor_label,
                                                    label_t edge_label) {
  size_t index = label * vertex_label_num_ * edge_label_num_ +
                 neighbor_label * edge_label_num_ + edge_label;
  return oe_[index];
}

const MutableCsrBase* MutablePropertyFragment::get_oe_csr(
    label_t label, label_t neighbor_label, label_t edge_label) const {
  size_t index = label * vertex_label_num_ * edge_label_num_ +
                 neighbor_label * edge_label_num_ + edge_label;
  return oe_[index];
}

MutableCsrBase* MutablePropertyFragment::get_ie_csr(label_t label,
                                                    label_t neighbor_label,
                                                    label_t edge_label) {
  size_t index = neighbor_label * vertex_label_num_ * edge_label_num_ +
                 label * edge_label_num_ + edge_label;
  return ie_[index];
}

const MutableCsrBase* MutablePropertyFragment::get_ie_csr(
    label_t label, label_t neighbor_label, label_t edge_label) const {
  size_t index = neighbor_label * vertex_label_num_ * edge_label_num_ +
                 label * edge_label_num_ + edge_label;
  return ie_[index];
}

}  // namespace gs
