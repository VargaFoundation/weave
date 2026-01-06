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
import org.springframework.util.unit.DataSize;
import reactor.core.publisher.Mono;
import varga.weave.core.ByteCapacity;
import varga.weave.core.Capacity;
import varga.weave.core.Infrastructure;
import varga.weave.core.InfrastructureManagerInputPort;
import varga.weave.core.TargetInfo;
import varga.weave.core.Tenant;

import java.time.Instant;

@Service
public class VirtualInfrastructureManagerAdapter implements InfrastructureManagerInputPort {

    @Override
    public Mono<TargetInfo> getSummaryInfo(Tenant tenant) {
        return findCapacityByInfrastructureById(tenant, "default");
    }

    @Override
    public Mono<Infrastructure> findInfrastructureById(Tenant tenant, String id) {
        Infrastructure infrastructure = new Infrastructure();
        infrastructure.setId(id);
        infrastructure.setName("Virtual Infrastructure " + id);
        infrastructure.setCreated(Instant.now());
        return Mono.just(infrastructure);
    }

    @Override
    public Mono<TargetInfo> findCapacityByInfrastructureById(Tenant tenant, String id) {
        TargetInfo targetInfo = new TargetInfo();
        targetInfo.setTotalCpus(new Capacity(0, 100, 50));
        targetInfo.setTotalMemory(new ByteCapacity(DataSize.ofGigabytes(0), DataSize.ofGigabytes(512), DataSize.ofGigabytes(256)));
        return Mono.just(targetInfo);
    }
}
