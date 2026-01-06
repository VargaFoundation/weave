package varga.weave.virtual;

/*-
 * #%L
 * Weave
 * %%
 * Copyright (C) 2025 - 2026 Varga Foundation
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import varga.weave.core.Tenant;
import varga.weave.core.User;
import varga.weave.core.UserRepositoryAdapter;

@Service
public class VirtualUserRepositoryAdapter implements UserRepositoryAdapter {

    @Override
    public Mono<User> findUserById(Tenant tenant, String userId) {
        User user = new User();
        user.setId(userId);
        user.setName("Virtual User " + userId);
        user.setEmail(userId + "@virtual.com");
        return Mono.just(user);
    }
}
