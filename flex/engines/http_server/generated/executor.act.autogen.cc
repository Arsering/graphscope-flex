/** Copyright 2021 Alibaba Group Holding Limited. All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "generated/executor_ref.act.autogen.h"
#include <hiactor/core/actor_client.hh>
#include <hiactor/core/actor_factory.hh>
#include <hiactor/util/data_type.hh>

namespace server {

enum : uint8_t {
	k_executor_run_graph_db_query = 0,
};

executor_ref::executor_ref() : ::hiactor::reference_base() { actor_type = 0; }

seastar::future<query_result> executor_ref::run_graph_db_query(server::query_param &&param) {
	addr.set_method_actor_tid(0);
	return hiactor::actor_client::request<server::query_result, server::query_param>(addr, k_executor_run_graph_db_query, std::forward<server::query_param>(param));
}

seastar::future<hiactor::stop_reaction> executor::do_work(hiactor::actor_message* msg) {
	switch (msg->hdr.behavior_tid) {
		case k_executor_run_graph_db_query: {
			static auto apply_run_graph_db_query = [] (hiactor::actor_message *a_msg, executor *self) {
				if (!a_msg->hdr.from_network) {
					return self->run_graph_db_query(std::move(reinterpret_cast<
						hiactor::actor_message_with_payload<server::query_param>*>(a_msg)->data));
				} else {
					auto* ori_msg = reinterpret_cast<hiactor::actor_message_with_payload<
						hiactor::serializable_queue>*>(a_msg);
					return self->run_graph_db_query(server::query_param::load_from(ori_msg->data));
				}
			};
			return apply_run_graph_db_query(msg, this).then_wrapped([msg] (seastar::future<server::query_result> fut) {
				if (__builtin_expect(fut.failed(), false)) {
					auto* ex_msg = hiactor::make_response_message(
						msg->hdr.src_shard_id,
						hiactor::simple_string::from_exception(fut.get_exception()),
						msg->hdr.pr_id,
						hiactor::message_type::EXCEPTION_RESPONSE);
					return hiactor::actor_engine().send(ex_msg);
				}
				auto* response_msg = hiactor::make_response_message(
					msg->hdr.src_shard_id, fut.get0(), msg->hdr.pr_id, hiactor::message_type::RESPONSE);
				return hiactor::actor_engine().send(response_msg);
			}).then([] {
				return seastar::make_ready_future<hiactor::stop_reaction>(hiactor::stop_reaction::no);
			});
		}
		default: {
			return seastar::make_ready_future<hiactor::stop_reaction>(hiactor::stop_reaction::yes);
		}
	}
}
} // namespace server

namespace auto_registration {

hiactor::registration::actor_registration<server::executor> _server_executor_auto_registration(0);

} // namespace auto_registration
