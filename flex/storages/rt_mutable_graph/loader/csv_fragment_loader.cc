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
#include "flex/storages/rt_mutable_graph/loader/csv_fragment_loader.h"
#include "flex/engines/hqps_db/core/utils/hqps_utils.h"
#include "flex/storages/rt_mutable_graph/loader/csv_reader_factory.h"

namespace gs {

static void check_edge_invariant(
    const Schema& schema,
    const std::vector<std::tuple<size_t, std::string, std::string>>&
        column_mappings,
    size_t src_col_ind, size_t dst_col_ind, label_t src_label_i,
    label_t dst_label_i, label_t edge_label_i) {
  // TODO(zhanglei): Check column mappings after multiple property on edge is
  // supported
  if (column_mappings.size() > 1) {
    LOG(FATAL) << "Edge column mapping must be less than 1";
  }
  if (column_mappings.size() > 0) {
    auto& mapping = column_mappings[0];
    if (std::get<0>(mapping) == src_col_ind ||
        std::get<0>(mapping) == dst_col_ind) {
      LOG(FATAL) << "Edge column mappings must not contain src_col_ind or "
                    "dst_col_ind";
    }
    auto src_label_name = schema.get_vertex_label_name(src_label_i);
    auto dst_label_name = schema.get_vertex_label_name(dst_label_i);
    auto edge_label_name = schema.get_edge_label_name(edge_label_i);
    // check property exists in schema
    if (!schema.edge_has_property(src_label_name, dst_label_name,
                                  edge_label_name, std::get<2>(mapping))) {
      LOG(FATAL) << "property " << std::get<2>(mapping)
                 << " not exists in schema for edge triplet " << src_label_name
                 << " -> " << edge_label_name << " -> " << dst_label_name;
    }
  }
}
#if OV
static void set_vertex_properties(gs::ColumnBase* col,
                                  std::shared_ptr<arrow::ChunkedArray> array,
                                  const std::vector<vid_t>& vids) {
  auto type = array->type();
  auto col_type = col->type();
  size_t cur_ind = 0;
  if (col_type == PropertyType::kInt64) {
    CHECK(type == arrow::int64())
        << "Inconsistent data type, expect int64, but got " << type->ToString();
    for (auto j = 0; j < array->num_chunks(); ++j) {
      auto casted =
          std::static_pointer_cast<arrow::Int64Array>(array->chunk(j));
      for (auto k = 0; k < casted->length(); ++k) {
        col->set_any(
            vids[cur_ind++],
            std::move(AnyConverter<int64_t>::to_any(casted->Value(k))));
      }
    }
  } else if (col_type == PropertyType::kInt32) {
    CHECK(type == arrow::int32())
        << "Inconsistent data type, expect int32, but got " << type->ToString();
    for (auto j = 0; j < array->num_chunks(); ++j) {
      auto casted =
          std::static_pointer_cast<arrow::Int32Array>(array->chunk(j));
      for (auto k = 0; k < casted->length(); ++k) {
        col->set_any(
            vids[cur_ind++],
            std::move(AnyConverter<int32_t>::to_any(casted->Value(k))));
      }
    }
  } else if (col_type == PropertyType::kDouble) {
    CHECK(type == arrow::float64())
        << "Inconsistent data type, expect double, but got "
        << type->ToString();
    for (auto j = 0; j < array->num_chunks(); ++j) {
      auto casted =
          std::static_pointer_cast<arrow::DoubleArray>(array->chunk(j));
      for (auto k = 0; k < casted->length(); ++k) {
        col->set_any(vids[cur_ind++],
                     std::move(AnyConverter<double>::to_any(casted->Value(k))));
      }
    }
  } else if (col_type == PropertyType::kString) {
    CHECK(type == arrow::large_utf8() || type == arrow::utf8())
        << "Inconsistent data type, expect string, but got "
        << type->ToString();
    if (type == arrow::large_utf8()) {
      for (auto j = 0; j < array->num_chunks(); ++j) {
        auto casted =
            std::static_pointer_cast<arrow::LargeStringArray>(array->chunk(j));
        for (auto k = 0; k < casted->length(); ++k) {
          auto str = casted->GetView(k);
          std::string_view str_view(str.data(), str.size());
          col->set_any(
              vids[cur_ind++],
              std::move(AnyConverter<std::string_view>::to_any(str_view)));
        }
      }
    } else {
      for (auto j = 0; j < array->num_chunks(); ++j) {
        auto casted =
            std::static_pointer_cast<arrow::StringArray>(array->chunk(j));
        for (auto k = 0; k < casted->length(); ++k) {
          auto str = casted->GetView(k);
          std::string_view str_view(str.data(), str.size());
          col->set_any(
              vids[cur_ind++],
              std::move(AnyConverter<std::string_view>::to_any(str_view)));
        }
      }
    }
  } else if (col_type == PropertyType::kDate) {
    if (type->Equals(arrow::int64())) {
      for (auto j = 0; j < array->num_chunks(); ++j) {
        auto casted =
            std::static_pointer_cast<arrow::Int64Array>(array->chunk(j));
        for (auto k = 0; k < casted->length(); ++k) {
          col->set_any(vids[cur_ind++],
                       std::move(AnyConverter<Date>::to_any(casted->Value(k))));
        }
      }
    } else if (type->Equals(arrow::timestamp(arrow::TimeUnit::MILLI))) {
      for (auto j = 0; j < array->num_chunks(); ++j) {
        auto casted =
            std::static_pointer_cast<arrow::TimestampArray>(array->chunk(j));
        for (auto k = 0; k < casted->length(); ++k) {
          col->set_any(vids[cur_ind++],
                       std::move(AnyConverter<Date>::to_any(casted->Value(k))));
        }
      }
    } else if (type->Equals(arrow::timestamp(arrow::TimeUnit::MICRO))) {
      for (auto j = 0; j < array->num_chunks(); ++j) {
        auto casted =
            std::static_pointer_cast<arrow::TimestampArray>(array->chunk(j));
        for (auto k = 0; k < casted->length(); ++k) {
          col->set_any(vids[cur_ind++],
                       std::move(AnyConverter<Date>::to_any(casted->Value(k))));
        }
      }
    } else {
      LOG(FATAL) << "Inconsistent data type, expect date, but got "
                 << type->ToString();
    }
  } else {
    LOG(FATAL) << "Not support type: " << type->ToString();
  }
}
#else
static void set_vertex_properties(gs::ColumnBase* col,
                                  std::shared_ptr<arrow::ChunkedArray> array,
                                  const std::vector<vid_t>& vids) {
  auto type = array->type();
  auto col_type = col->type();
  size_t cur_ind = 0;

  if (col_type == PropertyType::kInt64) {
    CHECK(type == arrow::int64())
        << "Inconsistent data type, expect int64, but got " << type->ToString();
    for (auto j = 0; j < array->num_chunks(); ++j) {
      auto casted =
          std::static_pointer_cast<arrow::Int64Array>(array->chunk(j));
      for (auto k = 0; k < casted->length(); ++k) {
        col->set_any(
            vids[cur_ind++],
            std::move(AnyConverter<int64_t>::to_any(casted->Value(k))));
      }
    }
  } else if (col_type == PropertyType::kInt32) {
    CHECK(type == arrow::int32())
        << "Inconsistent data type, expect int32, but got " << type->ToString();
    for (auto j = 0; j < array->num_chunks(); ++j) {
      auto casted =
          std::static_pointer_cast<arrow::Int32Array>(array->chunk(j));
      for (auto k = 0; k < casted->length(); ++k) {
        col->set_any(
            vids[cur_ind++],
            std::move(AnyConverter<int32_t>::to_any(casted->Value(k))));
      }
    }
  } else if (col_type == PropertyType::kDouble) {
    CHECK(type == arrow::float64())
        << "Inconsistent data type, expect double, but got "
        << type->ToString();
    for (auto j = 0; j < array->num_chunks(); ++j) {
      auto casted =
          std::static_pointer_cast<arrow::DoubleArray>(array->chunk(j));
      for (auto k = 0; k < casted->length(); ++k) {
        col->set_any(vids[cur_ind++],
                     std::move(AnyConverter<double>::to_any(casted->Value(k))));
      }
    }
  } else if (col_type == PropertyType::kString) {
    CHECK(type == arrow::large_utf8() || type == arrow::utf8())
        << "Inconsistent data type, expect string, but got "
        << type->ToString();
    if (type == arrow::large_utf8()) {
      for (auto j = 0; j < array->num_chunks(); ++j) {
        auto casted =
            std::static_pointer_cast<arrow::LargeStringArray>(array->chunk(j));
        for (auto k = 0; k < casted->length(); ++k) {
          auto str = casted->GetView(k);
          std::string_view str_view(str.data(), str.size());
          col->set_any(
              vids[cur_ind++],
              std::move(AnyConverter<std::string_view>::to_any(str_view)));
        }
      }
    } else {
      for (auto j = 0; j < array->num_chunks(); ++j) {
        auto casted =
            std::static_pointer_cast<arrow::StringArray>(array->chunk(j));
        for (auto k = 0; k < casted->length(); ++k) {
          auto str = casted->GetView(k);
          std::string_view str_view(str.data(), str.size());
          col->set_any(
              vids[cur_ind++],
              std::move(AnyConverter<std::string_view>::to_any(str_view)));
        }
      }
    }
  } else if (col_type == PropertyType::kDate) {
    if (type->Equals(arrow::int64())) {
      for (auto j = 0; j < array->num_chunks(); ++j) {
        auto casted =
            std::static_pointer_cast<arrow::Int64Array>(array->chunk(j));
        for (auto k = 0; k < casted->length(); ++k) {
          col->set_any(vids[cur_ind++],
                       std::move(AnyConverter<Date>::to_any(casted->Value(k))));
        }
      }
    } else if (type->Equals(arrow::timestamp(arrow::TimeUnit::MILLI))) {
      for (auto j = 0; j < array->num_chunks(); ++j) {
        auto casted =
            std::static_pointer_cast<arrow::TimestampArray>(array->chunk(j));
        for (auto k = 0; k < casted->length(); ++k) {
          col->set_any(vids[cur_ind++],
                       std::move(AnyConverter<Date>::to_any(casted->Value(k))));
        }
      }
    } else if (type->Equals(arrow::timestamp(arrow::TimeUnit::MICRO))) {
      for (auto j = 0; j < array->num_chunks(); ++j) {
        auto casted =
            std::static_pointer_cast<arrow::TimestampArray>(array->chunk(j));
        for (auto k = 0; k < casted->length(); ++k) {
          col->set_any(vids[cur_ind++],
                       std::move(AnyConverter<Date>::to_any(casted->Value(k))));
        }
      }
    } else {
      LOG(FATAL) << "Inconsistent data type, expect date, but got "
                 << type->ToString();
    }
  } else {
    LOG(FATAL) << "Not support type: " << type->ToString();
  }
}
#endif
template <typename EDATA_T>
static void append_edges(
    std::shared_ptr<arrow::Int64Array> src_col,
    std::shared_ptr<arrow::Int64Array> dst_col,
    const LFIndexer<vid_t>& src_indexer, const LFIndexer<vid_t>& dst_indexer,
    std::vector<std::shared_ptr<arrow::Array>>& edata_cols,
    std::vector<std::tuple<vid_t, vid_t, EDATA_T>>& parsed_edges,
    std::vector<int32_t>& ie_degree, std::vector<int32_t>& oe_degree) {
  CHECK(src_col->length() == dst_col->length());

  auto old_size = parsed_edges.size();
  parsed_edges.resize(old_size + src_col->length());
  VLOG(10) << "resize parsed_edges from " << old_size << " to "
           << parsed_edges.size();

  auto src_col_thread = std::thread([&]() {
    size_t cur_ind = old_size;
    for (auto i = 0; i < src_col->length(); ++i) {
      auto src_vid = src_indexer.get_index(src_col->Value(i));
      std::get<0>(parsed_edges[cur_ind++]) = src_vid;
      oe_degree[src_vid]++;
    }
  });
  auto dst_col_thread = std::thread([&]() {
    size_t cur_ind = old_size;
    for (auto i = 0; i < dst_col->length(); ++i) {
      auto dst_vid = dst_indexer.get_index(dst_col->Value(i));
      std::get<1>(parsed_edges[cur_ind++]) = dst_vid;
      ie_degree[dst_vid]++;
    }
  });
  src_col_thread.join();
  dst_col_thread.join();

  // if EDATA_T is grape::EmptyType, no need to read columns
  if constexpr (!std::is_same<EDATA_T, grape::EmptyType>::value) {
    CHECK(edata_cols.size() == 1);
    auto edata_col = edata_cols[0];
    CHECK(src_col->length() == edata_col->length());
    size_t cur_ind = old_size;
    auto type = edata_col->type();
    // if (type != CppTypeToArrowType<EDATA_T>::TypeValue()) {
    //   LOG(FATAL) << "Inconsistent data type, expect "
    //              << CppTypeToArrowType<EDATA_T>::TypeValue()->ToString()
    //              << ", but got " << type->ToString();
    // }

    using arrow_array_type =
        typename gs::CppTypeToArrowType<EDATA_T>::ArrayType;
    if (type->Equals(arrow::timestamp(arrow::TimeUnit::MICRO))) {
      auto casted_chunk = std::static_pointer_cast<arrow_array_type>(edata_col);
      for (auto j = 0; j < casted_chunk->length(); ++j) {
        std::get<2>(parsed_edges[cur_ind++]) = casted_chunk->Value(j);
      }
    } else if (type->Equals(arrow::timestamp(arrow::TimeUnit::MILLI))) {
      auto casted_chunk = std::static_pointer_cast<arrow_array_type>(edata_col);
      for (auto j = 0; j < casted_chunk->length(); ++j) {
        std::get<2>(parsed_edges[cur_ind++]) = casted_chunk->Value(j);
      }
    } else if (type->Equals(arrow::large_utf8()) ||
               type->Equals(arrow::utf8())) {
      auto casted_chunk = std::static_pointer_cast<arrow_array_type>(edata_col);
      for (auto j = 0; j < casted_chunk->length(); ++j) {
        std::get<2>(parsed_edges[cur_ind++]) = casted_chunk->GetView(j);
      }
    } else {
      auto casted_chunk = std::static_pointer_cast<arrow_array_type>(edata_col);
      for (auto j = 0; j < casted_chunk->length(); ++j) {
        std::get<2>(parsed_edges[cur_ind++]) = casted_chunk->Value(j);
      }
    }

    VLOG(10) << "Finish inserting:  " << src_col->length() << " edges";
  }
}

void CSVFragmentLoader::addVertexBatch(
    label_t v_label_id, IdIndexer<oid_t, vid_t>& indexer,
    std::shared_ptr<arrow::Array>& primary_key_col,
    const std::vector<std::shared_ptr<arrow::Array>>& property_cols) {
  size_t row_num = primary_key_col->length();
  CHECK_EQ(primary_key_col->type()->id(), arrow::Type::INT64);
  auto col_num = property_cols.size();
  for (size_t i = 0; i < col_num; ++i) {
    CHECK_EQ(property_cols[i]->length(), row_num);
  }
  auto casted_array =
      std::static_pointer_cast<arrow::Int64Array>(primary_key_col);
  std::vector<std::vector<Any>> prop_vec(property_cols.size());

  vid_t vid;
  std::vector<vid_t> vids;
  vids.reserve(row_num);

  for (auto i = 0; i < row_num; ++i) {
    if (!indexer.add(casted_array->Value(i), vid)) {
      LOG(FATAL) << "Duplicate vertex id: " << casted_array->Value(i) << " for "
                 << schema_.get_vertex_label_name(v_label_id);
    }
    vids.emplace_back(vid);
  }

  for (auto j = 0; j < property_cols.size(); ++j) {
    auto array = property_cols[j];
    auto chunked_array = std::make_shared<arrow::ChunkedArray>(array);
    set_vertex_properties(
        basic_fragment_loader_.GetVertexTable(v_label_id).column_ptrs()[j],
        chunked_array, vids);
  }

  VLOG(10) << "Insert rows: " << row_num;
}

void CSVFragmentLoader::addVerticesImpl(label_t v_label_id,
                                        const std::string& v_label_name,
                                        const std::vector<std::string> v_files,
                                        IdIndexer<oid_t, vid_t>& indexer) {
  VLOG(10) << "Parsing vertex file:" << v_files.size() << " for label "
           << v_label_name;

  for (auto& v_file : v_files) {
    auto vertex_column_mappings =
        loading_config_.GetVertexColumnMappings(v_label_id);
    auto primary_key = schema_.get_vertex_primary_key(v_label_id)[0];
    auto primary_key_name = std::get<1>(primary_key);
    size_t primary_key_ind = std::get<2>(primary_key);

    bool is_stream = !loading_config_.GetIsBatchReader();
    auto reader = create_vertex_reader(schema_, loading_config_, v_label_id,
                                       v_file, is_stream);

    while (true) {
      std::shared_ptr<arrow::RecordBatch> record_batch = reader->Read();
      if (record_batch == nullptr) {
        break;
      }
      auto columns = record_batch->columns();
      CHECK(primary_key_ind < columns.size());
      auto primary_key_column = columns[primary_key_ind];
      auto other_columns_array = columns;
      other_columns_array.erase(other_columns_array.begin() + primary_key_ind);
      VLOG(10) << "Reading record batch of size: " << record_batch->num_rows();
      addVertexBatch(v_label_id, indexer, primary_key_column,
                     other_columns_array);
    }
  }

  VLOG(10) << "Finish parsing vertex file:" << v_files.size() << " for label "
           << v_label_name;
}

void CSVFragmentLoader::addVertices(label_t v_label_id,
                                    const std::vector<std::string>& v_files) {
  auto primary_keys = schema_.get_vertex_primary_key(v_label_id);

  if (primary_keys.size() != 1) {
    LOG(FATAL) << "Only support one primary key for vertex.";
  }
  if (std::get<0>(primary_keys[0]) != PropertyType::kInt64) {
    LOG(FATAL) << "Only support int64_t primary key for vertex.";
  }

  std::string v_label_name = schema_.get_vertex_label_name(v_label_id);
  VLOG(10) << "Start init vertices for label " << v_label_name << " with "
           << v_files.size() << " files.";

  IdIndexer<oid_t, vid_t> indexer;

  addVerticesImpl(v_label_id, v_label_name, v_files, indexer);

  if (indexer.bucket_count() == 0) {
    indexer._rehash(schema_.get_max_vnum(v_label_name));
  }

  basic_fragment_loader_.FinishAddingVertex(v_label_id, indexer);

  VLOG(10) << "Finish init vertices for label " << v_label_name;
}

template <typename EDATA_T>
void CSVFragmentLoader::addEdgesImpl(label_t src_label_id, label_t dst_label_id,
                                     label_t e_label_id,
                                     const std::vector<std::string>& e_files) {
  auto edge_column_mappings = loading_config_.GetEdgeColumnMappings(
      src_label_id, dst_label_id, e_label_id);
  auto src_dst_col_pair =
      loading_config_.GetEdgeSrcDstCol(src_label_id, dst_label_id, e_label_id);
  if (src_dst_col_pair.first.size() != 1 ||
      src_dst_col_pair.second.size() != 1) {
    LOG(FATAL) << "We currently only support one src primary key and one "
                  "dst primary key";
  }
  size_t src_col_ind = src_dst_col_pair.first[0];
  size_t dst_col_ind = src_dst_col_pair.second[0];
  CHECK(src_col_ind != dst_col_ind);

  check_edge_invariant(schema_, edge_column_mappings, src_col_ind, dst_col_ind,
                       src_label_id, dst_label_id, e_label_id);

  std::vector<std::tuple<vid_t, vid_t, EDATA_T>> parsed_edges;
  std::vector<int32_t> ie_degree, oe_degree;
  const auto& src_indexer = basic_fragment_loader_.GetLFIndexer(src_label_id);
  const auto& dst_indexer = basic_fragment_loader_.GetLFIndexer(dst_label_id);
  ie_degree.resize(dst_indexer.size());
  oe_degree.resize(src_indexer.size());
  VLOG(10) << "src indexer size: " << src_indexer.size()
           << " dst indexer size: " << dst_indexer.size();

  for (auto filename : e_files) {
    VLOG(10) << "processing " << filename << " with src_col_id " << src_col_ind
             << " and dst_col_id " << dst_col_ind;
    bool is_stream = !loading_config_.GetIsBatchReader();
    auto reader =
        create_edge_reader(schema_, loading_config_, src_label_id, dst_label_id,
                           e_label_id, filename, is_stream);

    const auto& src_indexer = basic_fragment_loader_.GetLFIndexer(src_label_id);
    const auto& dst_indexer = basic_fragment_loader_.GetLFIndexer(dst_label_id);

    while (true) {
      std::shared_ptr<arrow::RecordBatch> record_batch = reader->Read();
      if (record_batch == nullptr) {
        break;
      }
      auto columns = record_batch->columns();
      CHECK(columns.size() >= 2);
      auto src_col = columns[0];
      auto dst_col = columns[1];
      CHECK(src_col->type() == arrow::int64())
          << "src_col type: " << src_col->type()->ToString();
      CHECK(dst_col->type() == arrow::int64())
          << "dst_col type: " << dst_col->type()->ToString();

      std::vector<std::shared_ptr<arrow::Array>> property_cols;
      for (auto i = 2; i < columns.size(); ++i) {
        property_cols.emplace_back(columns[i]);
      }
      CHECK(property_cols.size() <= 1)
          << "Currently only support at most one property on edge";
      {
        // add edges to vector
        CHECK(src_col->length() == dst_col->length());
        CHECK(src_col->type() == arrow::int64());
        CHECK(dst_col->type() == arrow::int64());
        auto src_casted_array =
            std::static_pointer_cast<arrow::Int64Array>(src_col);
        auto dst_casted_array =
            std::static_pointer_cast<arrow::Int64Array>(dst_col);
        append_edges(src_casted_array, dst_casted_array, src_indexer,
                     dst_indexer, property_cols, parsed_edges, ie_degree,
                     oe_degree);
      }
    }
  }

  basic_fragment_loader_.PutEdges(src_label_id, dst_label_id, e_label_id,
                                  parsed_edges, ie_degree, oe_degree);
  VLOG(10) << "Finish putting: " << parsed_edges.size() << " edges";
}

void CSVFragmentLoader::addEdges(label_t src_label_i, label_t dst_label_i,
                                 label_t edge_label_i,
                                 const std::vector<std::string>& filenames) {
  auto src_label_name = schema_.get_vertex_label_name(src_label_i);
  auto dst_label_name = schema_.get_vertex_label_name(dst_label_i);
  auto edge_label_name = schema_.get_edge_label_name(edge_label_i);
  if (filenames.size() <= 0) {
    LOG(FATAL) << "No edge files found for src label: " << src_label_name
               << " dst label: " << dst_label_name
               << " edge label: " << edge_label_name;
  }
  if (filenames.size() <= 0) {
    LOG(FATAL) << "No edge files found for src label: " << src_label_name
               << " dst label: " << dst_label_name
               << " edge label: " << edge_label_name;
  }
  VLOG(10) << "Init edges src label: " << src_label_name
           << " dst label: " << dst_label_name
           << " edge label: " << edge_label_name
           << " filenames: " << filenames.size();
  auto& property_types = schema_.get_edge_properties(
      src_label_name, dst_label_name, edge_label_name);
  size_t col_num = property_types.size();
  CHECK_LE(col_num, 1) << "Only single or no property is supported for edge.";

  if (col_num == 0) {
    if (filenames.empty()) {
      basic_fragment_loader_.AddNoPropEdgeBatch<grape::EmptyType>(
          src_label_i, dst_label_i, edge_label_i);
    } else {
      addEdgesImpl<grape::EmptyType>(src_label_i, dst_label_i, edge_label_i,
                                     filenames);
    }
  } else if (property_types[0] == PropertyType::kDate) {
    if (filenames.empty()) {
      basic_fragment_loader_.AddNoPropEdgeBatch<Date>(src_label_i, dst_label_i,
                                                      edge_label_i);
    } else {
      addEdgesImpl<Date>(src_label_i, dst_label_i, edge_label_i, filenames);
    }
  } else if (property_types[0] == PropertyType::kInt32) {
    if (filenames.empty()) {
      basic_fragment_loader_.AddNoPropEdgeBatch<int>(src_label_i, dst_label_i,
                                                     edge_label_i);
    } else {
      addEdgesImpl<int>(src_label_i, dst_label_i, edge_label_i, filenames);
    }
  } else if (property_types[0] == PropertyType::kInt64) {
    if (filenames.empty()) {
      basic_fragment_loader_.AddNoPropEdgeBatch<int64_t>(
          src_label_i, dst_label_i, edge_label_i);
    } else {
      addEdgesImpl<int64_t>(src_label_i, dst_label_i, edge_label_i, filenames);
    }
  } else if (property_types[0] == PropertyType::kString) {
    if (filenames.empty()) {
      basic_fragment_loader_.AddNoPropEdgeBatch<std::string>(
          src_label_i, dst_label_i, edge_label_i);
    } else {
      LOG(FATAL) << "Unsupported edge property type.";
    }
  } else if (property_types[0] == PropertyType::kDouble) {
    if (filenames.empty()) {
      basic_fragment_loader_.AddNoPropEdgeBatch<double>(
          src_label_i, dst_label_i, edge_label_i);
    } else {
      addEdgesImpl<double>(src_label_i, dst_label_i, edge_label_i, filenames);
    }
  } else {
    LOG(FATAL) << "Unsupported edge property type." << property_types[0];
  }
}

void CSVFragmentLoader::loadVertices() {
  auto vertex_sources = loading_config_.GetVertexLoadingMeta();
  if (vertex_sources.empty()) {
    LOG(INFO) << "Skip loading vertices since no vertex source is specified.";
    return;
  }

  if (thread_num_ == 1) {
    LOG(INFO) << "Loading vertices with single thread...";
    for (auto iter = vertex_sources.begin(); iter != vertex_sources.end();
         ++iter) {
      auto v_label_id = iter->first;
      auto v_files = iter->second;
      addVertices(v_label_id, v_files);
    }
  } else {
    // copy vertex_sources and edge sources to vector, since we need to
    // use multi-thread loading.
    std::vector<std::pair<label_t, std::vector<std::string>>> vertex_files;
    for (auto iter = vertex_sources.begin(); iter != vertex_sources.end();
         ++iter) {
      vertex_files.emplace_back(iter->first, iter->second);
    }
    LOG(INFO) << "Parallel loading with " << thread_num_ << " threads, " << " "
              << vertex_files.size() << " vertex files, ";
    std::atomic<size_t> v_ind(0);
    std::vector<std::thread> threads(thread_num_);
    for (int i = 0; i < thread_num_; ++i) {
      threads[i] = std::thread([&]() {
        while (true) {
          size_t cur = v_ind.fetch_add(1);
          if (cur >= vertex_files.size()) {
            break;
          }
          auto v_label_id = vertex_files[cur].first;
          addVertices(v_label_id, vertex_files[cur].second);
        }
      });
    }
    for (auto& thread : threads) {
      thread.join();
    }

    LOG(INFO) << "Finished loading vertices";
  }
}

void CSVFragmentLoader::loadEdges() {
  auto& edge_sources = loading_config_.GetEdgeLoadingMeta();

  if (edge_sources.empty()) {
    LOG(INFO) << "Skip loading edges since no edge source is specified.";
    return;
  }

  if (thread_num_ == 1) {
    LOG(INFO) << "Loading edges with single thread...";
    for (auto iter = edge_sources.begin(); iter != edge_sources.end(); ++iter) {
      auto& src_label_id = std::get<0>(iter->first);
      auto& dst_label_id = std::get<1>(iter->first);
      auto& e_label_id = std::get<2>(iter->first);
      auto& e_files = iter->second;

      addEdges(src_label_id, dst_label_id, e_label_id, e_files);
    }
  } else {
    std::vector<std::pair<typename LoadingConfig::edge_triplet_type,
                          std::vector<std::string>>>
        edge_files;
    for (auto iter = edge_sources.begin(); iter != edge_sources.end(); ++iter) {
      edge_files.emplace_back(iter->first, iter->second);
    }
    LOG(INFO) << "Parallel loading with " << thread_num_ << " threads, "
              << edge_files.size() << " edge files.";
    std::atomic<size_t> e_ind(0);
    std::vector<std::thread> threads(thread_num_);
    for (int i = 0; i < thread_num_; ++i) {
      threads[i] = std::thread([&]() {
        while (true) {
          size_t cur = e_ind.fetch_add(1);
          if (cur >= edge_files.size()) {
            break;
          }
          auto& edge_file = edge_files[cur];
          auto src_label_id = std::get<0>(edge_file.first);
          auto dst_label_id = std::get<1>(edge_file.first);
          auto e_label_id = std::get<2>(edge_file.first);
          auto& file_names = edge_file.second;
          addEdges(src_label_id, dst_label_id, e_label_id, file_names);
        }
      });
    }
    for (auto& thread : threads) {
      thread.join();
    }
    LOG(INFO) << "Finished loading edges";
  }
}

void CSVFragmentLoader::InitCGraph() {
  std::vector<std::vector<std::vector<cgraph::Vertex::ColumnConfiguration>>>
      column_configurations;
  column_configurations.resize(schema_.vprop_names_.size());
  std::vector<std::map<size_t, size_t>> edge_label_to_property_id;
  edge_label_to_property_id.resize(schema_.vprop_names_.size());
  std::unordered_map<gs::LoadingConfig::edge_triplet_type,
                     std::pair<size_t, size_t>,
                     boost::hash<gs::LoadingConfig::edge_triplet_type>>
      edge_property_ids;

  auto vertex_sources = loading_config_.GetVertexLoadingMeta();
  basic_fragment_loader_.cgraph_vertices_.reserve(schema_.vprop_names_.size());
  std::filesystem::create_directories(basic_fragment_loader_.work_dir_ +
                                      "/snapshots/" + std::to_string(1));
  std::vector<size_t> property_ids;
  property_ids.resize(schema_.vprop_names_.size(), 0);

  for (size_t vertex_id = 0; vertex_id < schema_.vprop_names_.size();
       vertex_id++) {
    LOG(INFO) << schema_.get_vertex_label_name(vertex_id);

    column_configurations[vertex_id].resize(
        schema_.vprop_column_family_nums_[vertex_id]);
    for (size_t column_id = 0;
         column_id < schema_.vproperties_[vertex_id].size(); column_id++) {
      column_configurations
          [vertex_id][schema_.vprop_column_family_ids_[vertex_id][column_id]]
              .emplace_back(
                  schema_.vprop_ids_[vertex_id][column_id],
                  schema_.vproperties_[vertex_id][column_id],
                  schema_.vprop_column_family_ids_[vertex_id][column_id],
                  PropertyType::kNone, schema_.get_vertex_label_name(vertex_id),
                  std::string(), cgraph::Vertex::EdgeDirection::kNone);
      if (property_ids[vertex_id] <= schema_.vprop_ids_[vertex_id][column_id]) {
        property_ids[vertex_id] = schema_.vprop_ids_[vertex_id][column_id] + 1;
      }
    }
  }

  // 获得edge的配置
  {
    const auto& edge_sources = loading_config_.GetEdgeLoadingMeta();
    edge_property_ids.reserve(edge_sources.size());

    for (auto iter = edge_sources.begin(); iter != edge_sources.end(); ++iter) {
      LOG(INFO) << "src_vertex_name: "
                << schema_.get_vertex_label_name(std::get<0>(iter->first))
                << " "
                << "dst_vertex_name: "
                << schema_.get_vertex_label_name(std::get<1>(iter->first))
                << " "
                << "e_label_name: "
                << schema_.get_edge_label_name(std::get<2>(iter->first));
      auto& src_label_id = std::get<0>(iter->first);
      auto& dst_label_id = std::get<1>(iter->first);
      auto& e_label_id = std::get<2>(iter->first);
      auto label_id =
          schema_.generate_edge_label(src_label_id, dst_label_id, e_label_id);

      // 获得ie的配置
      {
        auto ie_column_family_id = schema_.ie_column_family_.at(label_id);
        auto ie_column_type = PropertyType::kDynamicEdgeList;
        auto label_id_with_direction =
            schema_.generate_edge_label_with_direction(
                src_label_id, dst_label_id, e_label_id, false);
        if (schema_.ie_strategy_.at(label_id) == EdgeStrategy::kMultiple) {
          ie_column_type = PropertyType::kDynamicEdgeList;
        } else if (schema_.ie_strategy_.at(label_id) == EdgeStrategy::kSingle) {
          ie_column_type = PropertyType::kEdge;
        }
        if (schema_.ie_strategy_.at(label_id) != EdgeStrategy::kNone) {
          assert(schema_.eproperties_.at(label_id).size() <= 1);
          auto e_property_type = schema_.eproperties_.at(label_id).size() == 0
                                     ? PropertyType::kEmpty
                                     : schema_.eproperties_.at(label_id)[0];

          column_configurations[dst_label_id][ie_column_family_id].emplace_back(
              property_ids[dst_label_id], ie_column_type, ie_column_family_id,
              e_property_type, schema_.get_edge_label_name(label_id),
              schema_.get_vertex_label_name(src_label_id),
              cgraph::Vertex::EdgeDirection::kIn);
          edge_property_ids[iter->first].first = property_ids[dst_label_id]++;
          edge_label_to_property_id[dst_label_id][label_id_with_direction] =
              edge_property_ids[iter->first].first;
        }
      }

      // 获得oe的配置
      {
        auto oe_column_family_id = schema_.oe_column_family_.at(label_id);
        auto oe_column_type = PropertyType::kDynamicEdgeList;
        auto label_id_with_direction =
            schema_.generate_edge_label_with_direction(
                src_label_id, dst_label_id, e_label_id, true);
        if (schema_.oe_strategy_.at(label_id) == EdgeStrategy::kMultiple) {
          oe_column_type = PropertyType::kDynamicEdgeList;
        } else if (schema_.oe_strategy_.at(label_id) == EdgeStrategy::kSingle) {
          oe_column_type = PropertyType::kEdge;
        }
        if (schema_.oe_strategy_.at(label_id) != EdgeStrategy::kNone) {
          assert(schema_.eproperties_.at(label_id).size() <= 1);
          auto e_property_type = schema_.eproperties_.at(label_id).size() == 0
                                     ? PropertyType::kEmpty
                                     : schema_.eproperties_.at(label_id)[0];
          column_configurations[src_label_id][oe_column_family_id].emplace_back(
              property_ids[src_label_id], oe_column_type, oe_column_family_id,
              e_property_type, schema_.get_edge_label_name(label_id),
              schema_.get_vertex_label_name(dst_label_id),
              cgraph::Vertex::EdgeDirection::kOut);

          edge_property_ids[iter->first].second = property_ids[src_label_id]++;
          edge_label_to_property_id[src_label_id][label_id_with_direction] =
              edge_property_ids[iter->first].second;
        }
      }
    }
  }

  std::map<size_t, std::vector<std::pair<size_t, size_t>>>
      parent_configs;  // vertex_id -> (child_vertex_id, group_size)
  std::map<size_t, std::pair<size_t, size_t>>
      child_configs;  // vertex_id -> (parent_vertex_id, label_Id_in_parent,)
  for (size_t vertex_id = 0;
       vertex_id < schema_.vprop_column_family_nums_.size(); vertex_id++) {
    if (schema_.group_foreign_keys_1_.count(vertex_id) == 0) {
      continue;
    }
    auto& foreign_key_1 = schema_.group_foreign_keys_1_.at(vertex_id);
    auto& foreign_key_2 = schema_.group_foreign_keys_2_.at(vertex_id);
    auto child_vertex_id = vertex_id;
    auto group_size = std::get<1>(foreign_key_2);
    auto parent_vertex_id =
        schema_.get_vertex_label_id(std::get<0>(foreign_key_2));
    auto label_id_in_parent = parent_configs[parent_vertex_id].size();
    parent_configs[parent_vertex_id].emplace_back(child_vertex_id, group_size);
    child_configs[child_vertex_id] =
        std::make_pair(parent_vertex_id, label_id_in_parent);
  }

  // 创建vertex
  for (size_t vertex_id = 0;
       vertex_id < schema_.vprop_column_family_nums_.size(); vertex_id++) {
    // create vertex
    basic_fragment_loader_.cgraph_vertices_.emplace_back();

    // 初始化vertex
    basic_fragment_loader_.cgraph_vertices_[vertex_id].Init(
        column_configurations[vertex_id], edge_label_to_property_id[vertex_id],
        schema_.get_vertex_label_name(vertex_id),
        basic_fragment_loader_.work_dir_ + "/snapshots/" + std::to_string(1));
    // 初始化vertex的大小
    basic_fragment_loader_.cgraph_vertices_[vertex_id].Resize(
        schema_.get_max_vnum(schema_.get_vertex_label_name(vertex_id)));
  }

  cgraph_lf_indexers_.resize(schema_.vprop_names_.size());
  // 创建parent indexer and lf indexer
  for (size_t vertex_id = 0;
       vertex_id < schema_.vprop_column_family_nums_.size(); vertex_id++) {
    LOG(INFO) << schema_.get_vertex_label_name(vertex_id);
    auto v_files = vertex_sources[vertex_id];
    assert(v_files.size() == 1);
    csv::CSVLoader loader(v_files[0]);
    auto primary_key = loader.get_column(0);

    IdIndexer<oid_t, vid_t> indexer;  // 创建
    vid_t vid = 0;
    for (int i = 0; i < primary_key.size(); i++) {
      oid_t obj_id = std::stol(primary_key[i]);
      indexer.add(obj_id, vid);
      vid++;
    }
    std::string prefix = basic_fragment_loader_.work_dir_ + "/snapshots/" +
                         std::to_string(1) + "/" +
                         schema_.get_vertex_label_name(vertex_id) + "/hash_map";

    if (child_configs.count(vertex_id) == 1 &&
        parent_configs.count(vertex_id) == 1) {
      assert(false);
    }
    if (child_configs.count(vertex_id) == 0 &&
        parent_configs.count(vertex_id) == 0) {
      LFIndexer<vid_t>* lf_indexer = new LFIndexer<vid_t>();
      build_lf_indexer(indexer, prefix, *lf_indexer);
      cgraph_lf_indexers_[vertex_id] = lf_indexer;
    } else if (child_configs.count(vertex_id) == 1) {
      continue;
    } else {
      std::vector<size_t> group_sizes;
      for (auto& [child_vertex_id, group_size] : parent_configs[vertex_id]) {
        group_sizes.emplace_back(group_size);
      }
      cgraph_lf_indexers_[vertex_id] =
          GroupedParentLFIndexer_creation_helper(indexer, prefix, group_sizes);
    }
  }

  for (size_t vertex_id = 0;
       vertex_id < schema_.vprop_column_family_nums_.size(); vertex_id++) {
    LOG(INFO) << schema_.get_vertex_label_name(vertex_id);
    auto v_files = vertex_sources[vertex_id];
    assert(v_files.size() == 1);
    csv::CSVLoader loader(v_files[0]);
    auto primary_key = loader.get_column(0);

    IdIndexer<oid_t, vid_t> indexer;  // 创建
    vid_t vid = 0;
    for (int i = 0; i < primary_key.size(); i++) {
      oid_t obj_id = std::stol(primary_key[i]);
      indexer.add(obj_id, vid);
      vid++;
    }
    std::string prefix = basic_fragment_loader_.work_dir_ + "/snapshots/" +
                         std::to_string(1) + "/" +
                         schema_.get_vertex_label_name(vertex_id) + "/hash_map";

    if (child_configs.count(vertex_id) == 1 &&
        parent_configs.count(vertex_id) == 1) {
      assert(false);
    }
    if (child_configs.count(vertex_id) == 0 &&
        parent_configs.count(vertex_id) == 0) {
      continue;
    } else if (child_configs.count(vertex_id) == 1) {
      GroupedChildLFIndexer<vid_t>* lf_indexer =
          new GroupedChildLFIndexer<vid_t>();
      build_grouped_child_lf_indexer(
          indexer, prefix, *lf_indexer,
          *cgraph_lf_indexers_[child_configs[vertex_id].first],
          child_configs[vertex_id].second);
      cgraph_lf_indexers_[vertex_id] = lf_indexer;
      cgraph_lf_indexers_[child_configs[vertex_id].first]->set_new_kid_range(
          child_configs[vertex_id].second, indexer.size());
    } else {
      continue;
    }
  }

  // 插入property数据
  for (size_t vertex_id = 0;
       vertex_id < schema_.vprop_column_family_nums_.size(); vertex_id++) {
    LOG(INFO) << schema_.get_vertex_label_name(vertex_id);
    auto v_files = vertex_sources[vertex_id];
    assert(v_files.size() == 1);
    csv::CSVLoader loader(v_files[0]);
    auto primary_key = loader.get_column(0);

    for (auto column_family_id = 0;
         column_family_id < column_configurations[vertex_id].size();
         column_family_id++) {
      for (auto column_id = 0;
           column_id <
           column_configurations[vertex_id][column_family_id].size();
           column_id++) {
        auto& column_configuration =
            column_configurations[vertex_id][column_family_id][column_id];
        auto value = loader.get_column(column_configuration.property_id);

        switch (column_configuration.column_type) {
        case PropertyType::kInt32: {
          for (int k = 0; k < value.size(); k++) {
            int32_t data = std::stoi(value[k]);
            gs::vid_t vid = cgraph_lf_indexers_[vertex_id]->get_index(
                std::stol(primary_key[k]));
            basic_fragment_loader_.cgraph_vertices_[vertex_id].InsertColumn(
                vid, {column_configuration.property_id,
                      {reinterpret_cast<char*>(&data), sizeof(int32_t)}});
            auto result =
                basic_fragment_loader_.cgraph_vertices_[vertex_id].ReadColumn(
                    vid, column_configuration.property_id);
            assert(gbp::BufferBlock::Ref<int32_t>(result) == data);
          }
          break;
        }
        case PropertyType::kInt64: {
          for (int k = 0; k < value.size(); k++) {
            int64_t data = std::stoll(value[k]);
            gs::vid_t vid = cgraph_lf_indexers_[vertex_id]->get_index(
                std::stol(primary_key[k]));
            basic_fragment_loader_.cgraph_vertices_[vertex_id].InsertColumn(
                vid, {column_configuration.property_id,
                      {reinterpret_cast<char*>(&data), sizeof(int64_t)}});
            auto result =
                basic_fragment_loader_.cgraph_vertices_[vertex_id].ReadColumn(
                    vid, column_configuration.property_id);
            assert(gbp::BufferBlock::Ref<int64_t>(result) == data);
          }
          break;
        }
        case PropertyType::kDouble: {
          for (int k = 0; k < value.size(); k++) {
            double data = std::stod(value[k]);
            gs::vid_t vid = cgraph_lf_indexers_[vertex_id]->get_index(
                std::stol(primary_key[k]));
            basic_fragment_loader_.cgraph_vertices_[vertex_id].InsertColumn(
                vid, {column_configuration.property_id,
                      {reinterpret_cast<char*>(&data), sizeof(double)}});
            auto result =
                basic_fragment_loader_.cgraph_vertices_[vertex_id].ReadColumn(
                    vid, column_configuration.property_id);
            assert(gbp::BufferBlock::Ref<double>(result) == data);
          }
          break;
        }
        case PropertyType::kDate: {
          for (int k = 0; k < value.size(); k++) {
            gs::Date data;
            data.milli_second =
                gbp::TimeConverter::dateStringToMillis(value[k]);
            gs::vid_t vid = cgraph_lf_indexers_[vertex_id]->get_index(
                std::stol(primary_key[k]));
            basic_fragment_loader_.cgraph_vertices_[vertex_id].InsertColumn(
                vid, {column_configuration.property_id,
                      {reinterpret_cast<char*>(&data), sizeof(gs::Date)}});
            auto result =
                basic_fragment_loader_.cgraph_vertices_[vertex_id].ReadColumn(
                    vid, column_configuration.property_id);
            assert(gbp::BufferBlock::Ref<uint64_t>(result) ==
                   data.milli_second);
          }
          break;
        }
        case PropertyType::kString:
        case PropertyType::kDynamicString: {
          for (int k = 0; k < value.size(); k++) {
            std::string data = value[k];
            gs::vid_t vid = cgraph_lf_indexers_[vertex_id]->get_index(
                std::stol(primary_key[k]));
            basic_fragment_loader_.cgraph_vertices_[vertex_id].InsertColumn(
                vid, {column_configuration.property_id, data});
            auto result =
                basic_fragment_loader_.cgraph_vertices_[vertex_id].ReadColumn(
                    vid, column_configuration.property_id);
            if (!(result == data)) {
              LOG(INFO) << data;
            }
            assert(result == data);
          }
          break;
        }
        case PropertyType::kDynamicEdgeList:
        case PropertyType::kEdge: {
          break;
        }
        default: {
          LOG(FATAL) << "Unsupported property type."
                     << static_cast<int>(column_configuration.column_type);
        }
        }
      }
    }
  }

  // 插入edge数据
  {
    const auto& edge_sources = loading_config_.GetEdgeLoadingMeta();
    for (auto iter = edge_sources.begin(); iter != edge_sources.end(); ++iter) {
      LOG(INFO) << "src_vertex_name: "
                << schema_.get_vertex_label_name(std::get<0>(iter->first))
                << " "
                << "dst_vertex_name: "
                << schema_.get_vertex_label_name(std::get<1>(iter->first))
                << " "
                << "e_label_name: "
                << schema_.get_edge_label_name(std::get<2>(iter->first));
      auto& src_label_id = std::get<0>(iter->first);
      auto& dst_label_id = std::get<1>(iter->first);
      auto& e_label_id = std::get<2>(iter->first);
      auto label_id =
          schema_.generate_edge_label(src_label_id, dst_label_id, e_label_id);
      auto& e_files = iter->second;
      // 计算src和dst的度
      csv::CSVLoader loader(e_files[0]);
      std::vector<size_t> src_degree(cgraph_lf_indexers_[src_label_id]->size(),
                                     0);
      auto value = loader.get_column(0);
      for (auto i = 0; i < value.size(); i++) {
        oid_t obj_id = std::stol(value[i]);
        src_degree[cgraph_lf_indexers_[src_label_id]->get_index(obj_id)]++;
      }
      std::vector<size_t> dst_degree(cgraph_lf_indexers_[dst_label_id]->size(),
                                     0);
      value = loader.get_column(1);
      for (auto i = 0; i < value.size(); i++) {
        oid_t obj_id = std::stol(value[i]);
        dst_degree[cgraph_lf_indexers_[dst_label_id]->get_index(obj_id)]++;
      }

      auto vertex_num = 10;
      auto ie_property_id = edge_property_ids[iter->first].first;
      auto oe_property_id = edge_property_ids[iter->first].second;
      auto e_property_type = schema_.eproperties_.at(label_id).size() == 0
                                 ? PropertyType::kEmpty
                                 : schema_.eproperties_.at(label_id)[0];

      // 插入ie
      {
        auto label_id_with_direction =
            schema_.generate_edge_label_with_direction(
                src_label_id, dst_label_id, e_label_id, false);
        switch (schema_.ie_strategy_.at(label_id)) {
        case EdgeStrategy::kMultiple: {
          std::vector<std::pair<size_t, size_t>> edge_list_len;
          for (auto vertex_id = 0; vertex_id < dst_degree.size(); vertex_id++) {
            edge_list_len.emplace_back(vertex_id,
                                       (dst_degree[vertex_id] + 1) * 2);
          }

          basic_fragment_loader_.cgraph_vertices_[dst_label_id]
              .EdgeListInitBatch(label_id_with_direction, edge_list_len);
        }
        case EdgeStrategy::kSingle: {
          std::vector<char> edge;
          for (auto i = 0; i < loader.row_count(); i++) {
            std::string src_obj_id, dst_obj_id, property;
            src_obj_id = loader.get_element(i, 0);
            dst_obj_id = loader.get_element(i, 1);
            assert((loader.column_count() == 3) ==
                   (e_property_type != PropertyType::kEmpty));
            if (loader.column_count() == 3)
              property = loader.get_element(i, 2);
            assert(cgraph::Vertex::ConstructEdge(
                edge, property,
                cgraph_lf_indexers_[src_label_id]->get_index(
                    std::stol(src_obj_id)),
                e_property_type));
            basic_fragment_loader_.cgraph_vertices_[dst_label_id].InsertEdge(
                cgraph_lf_indexers_[dst_label_id]->get_index(
                    std::stol(dst_obj_id)),
                {label_id_with_direction, {edge.data(), edge.size()}});
          }
          break;
        }
        default: {
        }
        }
      }
      // 插入oe
      {
        auto label_id_with_direction =
            schema_.generate_edge_label_with_direction(
                src_label_id, dst_label_id, e_label_id, true);
        switch (schema_.oe_strategy_.at(label_id)) {
        case EdgeStrategy::kMultiple: {
          // 初始化oe
          std::vector<std::pair<size_t, size_t>> edge_list_len;
          for (auto vertex_id = 0; vertex_id < src_degree.size(); vertex_id++) {
            edge_list_len.emplace_back(vertex_id,
                                       (src_degree[vertex_id] + 1) * 2);
          }

          basic_fragment_loader_.cgraph_vertices_[src_label_id]
              .EdgeListInitBatch(label_id_with_direction, edge_list_len);
        }
        case EdgeStrategy::kSingle: {
          std::vector<char> edge;
          for (auto i = 0; i < loader.row_count(); i++) {
            std::string src_obj_id, dst_obj_id, property;
            src_obj_id = loader.get_element(i, 0);
            dst_obj_id = loader.get_element(i, 1);
            assert((loader.column_count() == 3) ==
                   (e_property_type != PropertyType::kEmpty));
            if (loader.column_count() == 3)
              property = loader.get_element(i, 2);
            assert(cgraph::Vertex::ConstructEdge(
                edge, property,
                cgraph_lf_indexers_[dst_label_id]->get_index(
                    std::stol(dst_obj_id)),
                e_property_type));
            basic_fragment_loader_.cgraph_vertices_[src_label_id].InsertEdge(
                cgraph_lf_indexers_[src_label_id]->get_index(
                    std::stol(src_obj_id)),
                {label_id_with_direction, {edge.data(), edge.size()}});
          }
          break;
        }
        default: {
        }
        }
      }
    }
  }

  {  // 保存schema
    std::string schema_filename =
        schema_path(basic_fragment_loader_.work_dir_ + "/snapshots/" +
                    std::to_string(1) + "/");
    auto io_adaptor = std::unique_ptr<grape::LocalIOAdaptor>(
        new grape::LocalIOAdaptor(schema_filename));
    io_adaptor->Open("wb");
    schema_.Serialize(io_adaptor);
    io_adaptor->Close();
  }
  LOG(INFO) << "load vertices done";
}

void CSVFragmentLoader::loadEdges_cgraph() {
  basic_fragment_loader_.cgraph_vertices_.reserve(schema_.vprop_names_.size());
  std::filesystem::create_directories(basic_fragment_loader_.work_dir_ +
                                      "/snapshots/" + std::to_string(1));

  std::vector<size_t> property_ids;
  std::vector<std::vector<std::vector<cgraph::Vertex::ColumnConfiguration>>>
      column_configurations;
  std::vector<std::vector<std::vector<std::string>>> edge_files;
  std::vector<std::map<size_t, size_t>> edge_label_to_property_id;
  property_ids.resize(schema_.vprop_names_.size());
  column_configurations.resize(schema_.vprop_names_.size());
  edge_label_to_property_id.resize(schema_.vprop_names_.size());
  for (size_t i = 0; i < schema_.vprop_column_family_nums_.size(); i++) {
    column_configurations[i].resize(schema_.vprop_column_family_nums_[i]);
    edge_files;
    property_ids[i] = *std::max_element(schema_.vprop_ids_[i].begin(),
                                        schema_.vprop_ids_[i].end()) +
                      1;
  }
  const auto& edge_sources = loading_config_.GetEdgeLoadingMeta();
  std::unordered_map<gs::LoadingConfig::edge_triplet_type,
                     std::pair<size_t, size_t>,
                     boost::hash<gs::LoadingConfig::edge_triplet_type>>
      edge_property_ids;
  edge_property_ids.reserve(edge_sources.size());
  for (auto iter = edge_sources.begin(); iter != edge_sources.end(); ++iter) {
    LOG(INFO) << "src_vertex_name: "
              << schema_.get_vertex_label_name(std::get<0>(iter->first)) << " "
              << "dst_vertex_name: "
              << schema_.get_vertex_label_name(std::get<1>(iter->first)) << " "
              << "e_label_name: "
              << schema_.get_edge_label_name(std::get<2>(iter->first));
    auto& src_label_id = std::get<0>(iter->first);
    auto& dst_label_id = std::get<1>(iter->first);
    auto& e_label_id = std::get<2>(iter->first);
    auto label_id =
        schema_.generate_edge_label(src_label_id, dst_label_id, e_label_id);

    // 获得ie的配置
    {
      auto ie_column_family_id = schema_.ie_column_family_.at(label_id);
      auto ie_column_type = PropertyType::kDynamicEdgeList;
      if (schema_.ie_strategy_.at(label_id) == EdgeStrategy::kMultiple) {
        ie_column_type = PropertyType::kDynamicEdgeList;
      } else if (schema_.ie_strategy_.at(label_id) == EdgeStrategy::kSingle) {
        ie_column_type = PropertyType::kEdge;
      }
      if (schema_.ie_strategy_.at(label_id) != EdgeStrategy::kNone) {
        assert(schema_.eproperties_.at(label_id).size() <= 1);
        auto e_property_type = schema_.eproperties_.at(label_id).size() == 0
                                   ? PropertyType::kEmpty
                                   : schema_.eproperties_.at(label_id)[0];

        column_configurations[dst_label_id][ie_column_family_id].emplace_back(
            property_ids[dst_label_id], ie_column_type, ie_column_family_id,
            e_property_type, schema_.get_edge_label_name(label_id),
            schema_.get_vertex_label_name(src_label_id),
            cgraph::Vertex::EdgeDirection::kIn);
        edge_property_ids[iter->first].first = property_ids[dst_label_id]++;
      }
    }

    // 获得oe的配置
    {
      auto oe_column_family_id = schema_.oe_column_family_.at(label_id);
      auto oe_column_type = PropertyType::kDynamicEdgeList;
      if (schema_.oe_strategy_.at(label_id) == EdgeStrategy::kMultiple) {
        oe_column_type = PropertyType::kDynamicEdgeList;
      } else if (schema_.oe_strategy_.at(label_id) == EdgeStrategy::kSingle) {
        oe_column_type = PropertyType::kEdge;
      }
      if (schema_.oe_strategy_.at(label_id) != EdgeStrategy::kNone) {
        assert(schema_.eproperties_.at(label_id).size() <= 1);
        auto e_property_type = schema_.eproperties_.at(label_id).size() == 0
                                   ? PropertyType::kEmpty
                                   : schema_.eproperties_.at(label_id)[0];
        column_configurations[src_label_id][oe_column_family_id].emplace_back(
            property_ids[src_label_id], oe_column_type, oe_column_family_id,
            e_property_type, schema_.get_edge_label_name(label_id),
            schema_.get_vertex_label_name(dst_label_id),
            cgraph::Vertex::EdgeDirection::kOut);
        edge_property_ids[iter->first].second = property_ids[src_label_id]++;
      }
    }
  }

  // 初始化column family
  for (auto vertex_id = 0; vertex_id < column_configurations.size();
       vertex_id++) {
    LOG(INFO) << schema_.get_vertex_label_name(vertex_id);
    auto& column_configuration = column_configurations[vertex_id];
    basic_fragment_loader_.cgraph_vertices_.emplace_back();
    basic_fragment_loader_.cgraph_vertices_.back().Init(
        column_configuration, edge_label_to_property_id[vertex_id],
        schema_.get_vertex_label_name(vertex_id),
        basic_fragment_loader_.work_dir_ + "/snapshots/" + std::to_string(1));

    basic_fragment_loader_.cgraph_vertices_.back().Resize(
        schema_.get_max_vnum(schema_.get_vertex_label_name(vertex_id)));
  }

  // 插入edge数据
  for (auto iter = edge_sources.begin(); iter != edge_sources.end(); ++iter) {
    LOG(INFO) << "src_vertex_name: "
              << schema_.get_vertex_label_name(std::get<0>(iter->first)) << " "
              << "dst_vertex_name: "
              << schema_.get_vertex_label_name(std::get<1>(iter->first)) << " "
              << "e_label_name: "
              << schema_.get_edge_label_name(std::get<2>(iter->first));
    auto& src_label_id = std::get<0>(iter->first);
    auto& dst_label_id = std::get<1>(iter->first);
    auto& e_label_id = std::get<2>(iter->first);
    auto label_id =
        schema_.generate_edge_label(src_label_id, dst_label_id, e_label_id);
    auto& e_files = iter->second;

    // csv::CSVLoader loader(e_files[0]);
    // std::map<size_t, size_t> counts;
    // auto value = loader.get_column(0);
    // for (auto i = 0; i < value.size(); i++) {
    //   oid_t obj_id = std::stol(value[i]);
    //   auto it = counts.find(obj_id);
    //   if (it == counts.end()) {
    //     counts[obj_id] = 1;
    //   } else {
    //     it->second++;
    //   }
    // }
    auto vertex_num = 10;
    auto ie_property_id = edge_property_ids[iter->first].first;
    auto oe_property_id = edge_property_ids[iter->first].second;
    auto e_property_type = schema_.eproperties_.at(label_id).size() == 0
                               ? PropertyType::kEmpty
                               : schema_.eproperties_.at(label_id)[0];
    std::array<uint64_t, 3> edge_list = {10, 100, 299};  // 假数据
    // 插入ie
    if (schema_.ie_strategy_.at(label_id) == EdgeStrategy::kMultiple) {
      std::vector<std::pair<size_t, size_t>> edge_list_len;
      for (auto vertex_id = 0; vertex_id < vertex_num; vertex_id++) {
        edge_list_len.push_back({vertex_id, edge_list.size()});
      }

      basic_fragment_loader_.cgraph_vertices_[dst_label_id].EdgeListInitBatch(
          ie_property_id, edge_list_len);

      std::vector<char> edge;

      assert(cgraph::Vertex::ConstructEdge(edge, std::string(), edge_list[0],
                                           e_property_type));

      basic_fragment_loader_.cgraph_vertices_[dst_label_id].InsertEdge(
          0, {ie_property_id, {edge.data(), edge.size()}});

    } else if (schema_.ie_strategy_.at(label_id) == EdgeStrategy::kSingle) {
      std::vector<char> edge;
      assert(cgraph::Vertex::ConstructEdge(edge, std::string(), edge_list[0],
                                           e_property_type));
      basic_fragment_loader_.cgraph_vertices_[dst_label_id].InsertColumn(
          0, {ie_property_id, {edge.data(), edge.size()}});
    } else {
      assert(ie_property_id == 0);
    }

    // 插入oe
    if (schema_.oe_strategy_.at(label_id) == EdgeStrategy::kMultiple) {
      std::vector<std::pair<size_t, size_t>> edge_list_len;
      for (auto vertex_id = 0; vertex_id < vertex_num; vertex_id++) {
        edge_list_len.push_back({vertex_id, edge_list.size()});
      }

      basic_fragment_loader_.cgraph_vertices_[src_label_id].EdgeListInitBatch(
          oe_property_id, edge_list_len);
      std::vector<char> edge;

      assert(cgraph::Vertex::ConstructEdge(edge, std::string(), edge_list[0],
                                           e_property_type));

      basic_fragment_loader_.cgraph_vertices_[src_label_id].InsertEdge(
          0, {oe_property_id, {edge.data(), edge.size()}});

    } else if (schema_.oe_strategy_.at(label_id) == EdgeStrategy::kSingle) {
      std::vector<char> edge;
      assert(cgraph::Vertex::ConstructEdge(edge, std::string(), edge_list[0],
                                           e_property_type));
      basic_fragment_loader_.cgraph_vertices_[src_label_id].InsertColumn(
          0, {oe_property_id, {edge.data(), edge.size()}});
    } else {
      assert(oe_property_id == 0);
    }
  }
  LOG(INFO) << "load edges done";
}

void CSVFragmentLoader::OpenCGraph() {  // 创建vertex

  std::filesystem::create_directories(basic_fragment_loader_.work_dir_ +
                                      "/runtime/tmp");  // 创建runtime/tmp目录
  LOG(INFO) << "copy snapshot from "
            << basic_fragment_loader_.work_dir_ + "/snapshots/" +
                   std::to_string(1)
            << " to " << basic_fragment_loader_.work_dir_ + "/runtime/tmp";
  std::filesystem::copy(
      basic_fragment_loader_.work_dir_ + "/snapshots/" + std::to_string(1),
      basic_fragment_loader_.work_dir_ + "/runtime/tmp",
      std::filesystem::copy_options::recursive |
          std::filesystem::copy_options::overwrite_existing);
  LOG(INFO) << "copy snapshot to tmp done";

  for (size_t vertex_id = 0;
       vertex_id < schema_.vprop_column_family_nums_.size(); vertex_id++) {
    basic_fragment_loader_.lf_indexers_.emplace_back();
    basic_fragment_loader_.lf_indexers_.back().open(
        "hash_map",
        basic_fragment_loader_.work_dir_ + "/snapshots/" + std::to_string(1) +
            "/" + schema_.get_vertex_label_name(vertex_id),
        basic_fragment_loader_.work_dir_ + "/runtime/tmp/" +
            schema_.get_vertex_label_name(vertex_id));
    // create vertex
    basic_fragment_loader_.cgraph_vertices_.emplace_back();
    // 初始化vertex
    basic_fragment_loader_.cgraph_vertices_.back().Open(
        schema_.get_vertex_label_name(vertex_id),
        basic_fragment_loader_.work_dir_ + "/runtime/tmp");
  }

  LOG(INFO) << "open vertex done";

  auto person_label_id = schema_.get_vertex_label_id("PERSON");
  auto property_id = 5;
  gs::oid_t person_oid = 8796093022290;
  gs::vid_t person_vid =
      basic_fragment_loader_.cgraph_indexers_[person_label_id]->get_index(
          person_oid);
  auto item =
      basic_fragment_loader_.cgraph_vertices_[person_label_id].ReadColumn(
          person_vid, property_id);
  std::vector<char> data(item.Size());
  item.Copy(data.data(), data.size());
  LOG(INFO) << "data: " << std::string_view(data.data(), data.size());
  LOG(INFO) << "data: " << gbp::BufferBlock::Ref<gs::Date>(item).milli_second;
  LOG(INFO) << gbp::TimeConverter::millisToDateString(
      gbp::BufferBlock::Ref<gs::Date>(item).milli_second, true);

  size_t edge_size = 10;
  auto comment_label_id = schema_.get_vertex_label_id("COMMENT");
  auto edge_label_id_with_direction3 =
      schema_.generate_edge_label_with_direction(
          comment_label_id, person_label_id,
          schema_.get_edge_label_id("HASCREATOR"), false);
  gs::vid_t person_vid2 =
      basic_fragment_loader_.cgraph_indexers_[person_label_id]->get_index(933);
  auto item_t3 =
      basic_fragment_loader_.cgraph_vertices_[person_label_id].ReadEdges(
          person_vid2, edge_label_id_with_direction3, edge_size);
  for (int i = 0; i < edge_size; i++) {
    LOG(INFO)
        << basic_fragment_loader_.cgraph_indexers_[comment_label_id]->get_key(
               gbp::BufferBlock::Ref<MutableNbr<grape::EmptyType>>(item_t3, i)
                   .neighbor)
        << " "
        << gbp::BufferBlock::Ref<MutableNbr<grape::EmptyType>>(item_t3, i)
               .neighbor;
  }
}

void CSVFragmentLoader::LoadFragment() {
  InitCGraph();
  // OpenCGraph();
  // loadEdges_cgraph();
  return;
  loadVertices();
  loadEdges();

  basic_fragment_loader_.LoadFragment();
}
}  // namespace gs
