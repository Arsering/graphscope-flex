
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

#ifndef STORAGES_RT_MUTABLE_GRAPH_LOADER_CSV_FRAGMENT_LOADER_H_
#define STORAGES_RT_MUTABLE_GRAPH_LOADER_CSV_FRAGMENT_LOADER_H_

#include "flex/storages/rt_mutable_graph/loader/basic_fragment_loader.h"
#include "flex/storages/rt_mutable_graph/loader/i_fragment_loader.h"
#include "flex/storages/rt_mutable_graph/loading_config.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"

#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include "arrow/util/value_parsing.h"

#include "grape/util.h"
#include "flex/storages/rt_mutable_graph/loader/edge_reader.h"
namespace gs {

// LoadFragment for csv files.
class CSVFragmentLoader : public IFragmentLoader {
 public:
  CSVFragmentLoader(const std::string& work_dir, const Schema& schema,
                    const LoadingConfig& loading_config, int32_t thread_num)
      : loading_config_(loading_config),
        schema_(schema),
        thread_num_(thread_num),
        basic_fragment_loader_(schema_, work_dir) {
    vertex_label_num_ = schema_.vertex_label_num();
    edge_label_num_ = schema_.edge_label_num();
  }

  ~CSVFragmentLoader() {}

  FragmentLoaderType GetFragmentLoaderType() const override {
    return FragmentLoaderType::kCSVFragmentLoader;
  }

  void LoadFragment() override;

  void test_csv_loader_vertex();
  void test_csv_loader_edge();

 private:
  void loadVertices();

  void loadEdges();

  void addVertices(label_t v_label_id, const std::vector<std::string>& v_files);

  void addVerticesImpl(label_t v_label_id, const std::string& v_label_name,
                       const std::vector<std::string> v_file,
                       IdIndexer<oid_t, vid_t>& indexer);

  void addVertexBatch(
      label_t v_label_id, IdIndexer<oid_t, vid_t>& indexer,
      std::shared_ptr<arrow::Array>& primary_key_col,
      const std::vector<std::shared_ptr<arrow::Array>>& property_cols);

  void addEdges(label_t src_label_id, label_t dst_label_id, label_t e_label_id,
                const std::vector<std::string>& e_files);

  template <typename EDATA_T>
  void addEdgesImpl(label_t src_label_id, label_t dst_label_id,
                    label_t e_label_id,
                    const std::vector<std::string>& e_files);
  void LoadCGraph();
  void loadIndexer_cgraph(size_t vertex_label_id,
                          IdIndexer<oid_t, vid_t>& indexer);

  void loadVertices_cgraph(
      size_t vertex_label_id,
      std::vector<std::vector<gs::cgraph::Vertex::ColumnConfiguration>>&
          column_configurations);

  void loadEdges_cgraph();

  void loadCreatorEdges(label_t input_src_label_id,std::unordered_map<int64_t, int64_t>* message_to_person_map){
    LOG(INFO)<<"load creator edges";
    auto edge_sources=loading_config_.GetEdgeLoadingMeta();
    for (auto iter = edge_sources.begin(); iter != edge_sources.end(); ++iter) {
      auto& src_label_id = std::get<0>(iter->first);
      auto& dst_label_id = std::get<1>(iter->first);
      auto& e_label_id = std::get<2>(iter->first);
      if(src_label_id==input_src_label_id)
      {
        if(dst_label_id==schema_.get_vertex_label_id("PERSON"))
        {
          if(e_label_id==schema_.get_edge_label_id("HASCREATOR"))
          {
            auto& e_files = iter->second;
            for(auto& e_file:e_files){
              assert(e_files.size()==1);
              EdgeReader reader;
              reader.build_comment_to_person_map(e_file, message_to_person_map);
            }
          }
        }
      }
    }
  } 

  void OpenCGraph();

  const LoadingConfig& loading_config_;
  const Schema& schema_;
  size_t vertex_label_num_, edge_label_num_;
  int32_t thread_num_;

  mutable BasicFragmentLoader basic_fragment_loader_;

  std::vector<LFIndexer<vid_t>> lf_indexers_;
  std::vector<cgraph::Vertex> cgraph_vertices_;
  std::vector<BaseIndexer<vid_t>*> cgraph_lf_indexers_;
  
};

}  // namespace gs

#endif  // STORAGES_RT_MUTABLE_GRAPH_LOADER_CSV_FRAGMENT_LOADER_H_