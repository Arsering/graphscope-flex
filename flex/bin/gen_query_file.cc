#include <fstream>
#include <iostream>
#include <random>
#include <utility>
#include <vector>

#include <boost/program_options.hpp>
#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"

namespace bpo = boost::program_options;

class CSVReader {
  std::ifstream csv_data_;
  std::vector<std::string> words_;

 public:
  CSVReader() = default;
  CSVReader(const std::string& file_name) { init(file_name); }

  ~CSVReader() = default;

  void init(const std::string& file_name) {
    try {
      csv_data_.open(file_name, std::ios::in);
    } catch (std::ios_base::failure& e) {
      std::cout << "fuck" << std::endl;
      return;
    }
    LOG(INFO) << "file_name=" << file_name;
    std::string line;
    std::getline(csv_data_, line);  // ignore the first line of file
  }

  std::vector<std::string>& GetNextLine() {
    words_.clear();
    std::string line, word;
    if (!std::getline(csv_data_, line))
      return words_;

    std::istringstream sin;
    sin.clear();
    sin.str(line);

    while (std::getline(sin, word, '|')) {
      words_.push_back(word);
    }
    return words_;
  }
};

class QueryFactory {
 private:
  int query_type = -1;
  std::string csv_dir_path;
  std::string file_name;
};

std::vector<std::string> gen_query(const std::string& csv_dir_path,
                                   int query_id) {
  std::vector<std::string> result_buffer;
  std::vector<char> tmp;
  CSVReader csv_reader;
  if (query_id < 4)
    csv_reader.init(csv_dir_path + "/dynamic/person_0_0.csv");
  else
    csv_reader.init(csv_dir_path + "/dynamic/post_0_0.csv");

  while (true) {
    gs::Encoder encoder(tmp);
    auto& words = csv_reader.GetNextLine();
    if (words.size() == 0)
      break;

    encoder.put_long(stol(words[0]));
    encoder.put_byte(query_id + 14);
    result_buffer.emplace_back(std::string(tmp.begin(), tmp.end()));
    tmp.clear();
  }
  // LOG(INFO) << csv_reader << std::endl;
  return std::move(result_buffer);
}

template <typename T>
void shuffle_mine(std::vector<T>& input) {
  std::random_device rd;
  std::mt19937 rng(rd());

  std::shuffle(input.begin(), input.end(), rng);
}

std::vector<size_t> create_random_order(size_t len) {
  std::vector<size_t> order_output(len);
  std::iota(order_output.begin(), order_output.end(), 0);
  shuffle_mine(order_output);
  return std::move(order_output);
}

int main(int argc, char** argv) {
  bpo::options_description desc("Usage:");
  desc.add_options()("csv-data-path,c", bpo::value<std::string>(),
                     "csv data directory path")(
      "output-data-path,o", bpo::value<std::string>(), "output directory path");
  google::InitGoogleLogging(argv[0]);
  FLAGS_logtostderr = true;

  bpo::variables_map vm;
  bpo::store(bpo::command_line_parser(argc, argv).options(desc).run(), vm);
  bpo::notify(vm);

  if (vm.count("help")) {
    std::cout << desc << std::endl;
    return 0;
  }

  std::string csv_data_path = "";
  std::string output_data_path = "";

  if (!vm.count("csv-data-path")) {
    LOG(ERROR) << "csv-data-path is required";
    return -1;
  }
  csv_data_path = vm["csv-data-path"].as<std::string>();

  if (!vm.count("output-data-path")) {
    LOG(ERROR) << "output-data-path is required";
    return -1;
  }
  output_data_path = vm["output-data-path"].as<std::string>();

  std::string query_file_name = output_data_path + "/query.file";
  std::ofstream query_file(query_file_name, std::ios::out);

  std::vector<std::vector<std::string>> queries;
  size_t overall_size = 0;
  std::vector<size_t> size_p;
  for (int query_id = 1; query_id < 8; query_id++) {
    auto queries_1 = gen_query(csv_data_path, query_id);
    overall_size += queries_1.size();
    queries.push_back(std::move(queries_1));
    size_p.push_back(overall_size);
  }

  auto orders = create_random_order(overall_size);
  std::string end_mark = "eor#";
  size_t count = 0;
  for (auto i : orders) {
    std::string* data_ptr;
    if (i < size_p[0])
      data_ptr = &(queries[0][i]);
    else if (i < size_p[1])
      data_ptr = &(queries[1][i - size_p[0]]);
    else if (i < size_p[2])
      data_ptr = &(queries[2][i - size_p[1]]);
    else if (i < size_p[3])
      data_ptr = &(queries[3][i - size_p[2]]);
    else if (i < size_p[4])
      data_ptr = &(queries[4][i - size_p[3]]);
    else if (i < size_p[5])
      data_ptr = &(queries[5][i - size_p[4]]);
    else if (i < size_p[6])
      data_ptr = &(queries[6][i - size_p[5]]);
    else
      LOG(FATAL) << "Great Error";
    query_file.write(data_ptr->data(), data_ptr->size());
    // LOG(INFO) << "queries_1[i].data()"
    //           << *reinterpret_cast<size_t*>(queries_1[i].data());
    query_file.write(end_mark.data(), end_mark.size());
    query_file << count++;
    query_file.write(end_mark.data(), end_mark.size());

    // if (--count == 0)
    //   break;
  }
  LOG(INFO) << "Number of query = " << overall_size;
  query_file.flush();
  query_file.close();
  // LOG(INFO) << "file size = " << std::filesystem::file_size(query_file_name);
}