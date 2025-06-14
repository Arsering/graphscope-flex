// /** Copyright 2020 Alibaba Group Holding Limited.
//  *
//  * Licensed under the Apache License, Version 2.0 (the "License");
//  * you may not use this file except in compliance with the License.
//  * You may obtain a copy of the License at
//  *
//  * 	http://www.apache.org/licenses/LICENSE-2.0
//  *
//  * Unless required by applicable law or agreed to in writing, software
//  * distributed under the License is distributed on an "AS IS" BASIS,
//  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  * See the License for the specific language governing permissions and
//  * limitations under the License.
//  */
// #ifndef ENGINES_HTTP_SERVER_HQPS_HTTP_HANDLER_H_
// #define ENGINES_HTTP_SERVER_HQPS_HTTP_HANDLER_H_

// #include <seastar/http/httpd.hh>

// namespace server {

// class hqps_http_handler {
//  public:
//   hqps_http_handler(uint16_t http_port);

//   void start();
//   void stop();

//  private:
//   seastar::future<> set_routes();

//  private:
//   const uint16_t http_port_;
//   seastar::httpd::http_server_control server_;
// };

// }  // namespace server

// #endif  // ENGINES_HTTP_SERVER_HQPS_HTTP_HANDLER_H_