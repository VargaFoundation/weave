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


import varga.weave.core.Tenant;
import varga.weave.rpc.IdUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationAttemptIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerProto;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class VirtualContainerRepository { // TODO use persistent storage

    private Map<String, ContainerProto> map = new HashMap<>();
    private AtomicLong uniqueContainerCounter = new AtomicLong();

    public Mono<ContainerProto> getAndRemoveContainer(Tenant tenant, String containerId) {
        return Mono.justOrEmpty(this.map.remove(containerId));
    }

    public Mono<Void> getContainers(Tenant tenant, List<ContainerProto> containers) {
        return Mono.fromCallable(() -> {
            Map<String, ContainerProto> collect = containers.stream().collect(Collectors.toMap(c -> IdUtils.getContainerId(c.getId()), c -> c));
            this.map.putAll(collect); // TODO use tenant in map
            return null;
        });
    }

    public Mono<Void> saveContainer(Tenant tenant, List<ContainerProto> containers) {
        return Mono.fromCallable(() -> {
            Map<String, ContainerProto> collect = containers.stream().collect(Collectors.toMap(c -> IdUtils.getContainerId(c.getId()), c -> c));
            this.map.putAll(collect); // TODO use tenant in map
            return null;
        });
    }

    // TODO use tenant in map
    public Mono<ContainerIdProto> nextContainerId(Tenant tenant, ApplicationIdProto appId) {
        return Mono.just(ContainerIdProto.newBuilder()
                .setAppId(appId)
                .setAppAttemptId(ApplicationAttemptIdProto.newBuilder()
                        .setApplicationId(appId)
                        .setAttemptId(0)
                        .build())
                .setId(this.uniqueContainerCounter.getAndIncrement()) // TODO use uniqueness per tenant/app
                .build()
        );
    }
}

