package varga.weave.virtual;

/*-
 * #%L
 * Weave
 * %%
 * Copyright (C) 2025 Varga Foundation
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


import varga.weave.core.Tenant;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

@Slf4j
@Service
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class VirtualClusterRepository { // TODO use persistent storage

    private Map<String, Long> map = new HashMap<>();
    private Map<Long, String> reverseMap = new HashMap<>();

    public Mono<Long> getClusterId(Tenant tenant) {
        return Mono.fromCallable(() -> this.map.computeIfAbsent(tenant.getId(), k -> {
            long clusterId = Math.abs(new Random().nextLong());
            this.reverseMap.put(clusterId, tenant.getId());
            return clusterId;
        }));
    }

    public Mono<String> getTenantId(Long clusterId) {
        return Mono.just(this.reverseMap.get(clusterId));
    }
}

