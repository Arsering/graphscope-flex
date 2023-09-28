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

#include "executor_group.actg.h"
#include <hiactor/core/actor_factory.hh>

template <>
uint16_t hiactor::get_actor_group_type_id<server::executor_group>() {
	return 61442;
}

namespace auto_registration {

hiactor::registration::actor_registration<server::executor_group> _server_executor_group_auto_registration(61442);

} // namespace auto_registration
