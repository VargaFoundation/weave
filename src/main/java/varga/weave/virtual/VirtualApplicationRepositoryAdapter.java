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
import varga.weave.core.ApplicationRepositoryAdapter;
import varga.weave.rpc.protocol.RpcApplication;

@Service
public class VirtualApplicationRepositoryAdapter implements ApplicationRepositoryAdapter {

    @Override
    public Mono<RpcApplication> findApplicationById(Tenant tenant, String applicationKey) {
        RpcApplication app = new RpcApplication();
        app.setId(applicationKey);
        app.setUserId("virtual-user-id");
        app.setRemoteApplicationSecret("virtual-secret");
        return Mono.just(app);
    }
}
