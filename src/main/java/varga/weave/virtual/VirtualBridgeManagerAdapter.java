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
import varga.weave.core.Bridge;
import varga.weave.core.BridgeManagerInputPort;
import varga.weave.core.Tenant;

import java.time.Instant;

@Service
public class VirtualBridgeManagerAdapter implements BridgeManagerInputPort {

    @Override
    public Mono<Bridge> findBridgeById(Tenant tenant, String id) {
        Bridge bridge = new Bridge();
        bridge.setId(id);
        bridge.setType("yarn");
        bridge.setCreated(Instant.now());
        return Mono.just(bridge);
    }
}
