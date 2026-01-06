package varga.weave.rpc.protocol;

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

import varga.weave.core.StandardErrorCodes;
import varga.weave.core.AuthenticationType;
import varga.weave.core.InternalAuthentication;
import varga.weave.core.Tenant;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.context.ReactiveSecurityContextHolder;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Slf4j
@Component
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class RpcAuthenticationManager {

    private final UserRepositoryAdapter userRepositoryAdapter;
    private final ApplicationRepositoryAdapter applicationRepositoryAdapter;

    public <T> Mono<T> withAuthentication(Tenant tenant, com.google.protobuf.MessageOrBuilder req, Mono<T> mono) {
        String applicationKey = ProtocolUtils.getApplicationKey(req.getUnknownFields());
        String applicationSecret = ProtocolUtils.getApplicationSecret(req.getUnknownFields());

        return
                this.applicationRepositoryAdapter.findApplicationById(tenant, applicationKey)
                        .flatMap(application -> {
                            String userId = application.getUserId();
                            String remoteApplicationSecret = application.getRemoteApplicationSecret();
                            if (!applicationSecret.equals(remoteApplicationSecret)) {
                                return Mono.error(StandardErrorCodes.UNAUTHORIZED.wrapAsException());
                            }

                            return this.userRepositoryAdapter.findUserById(tenant, userId)
                                    .flatMap(user -> {
                                        InternalAuthentication internalAuthentication = new InternalAuthentication(tenant.getId(), AuthenticationType.USER, user, null, null, false, null);
                                        return mono
                                                .contextWrite(ReactiveSecurityContextHolder.withAuthentication(internalAuthentication));
                                    })
                                    .switchIfEmpty(Mono.error(StandardErrorCodes.UNAUTHORIZED.wrapAsException()));
                        });
    }
}
