package varga.weave.api;

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

import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.server.ServerRequest;
import reactor.core.publisher.Mono;
import varga.weave.rest.NewApplication;
import varga.weave.rest.YarnApplication;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@Component
public class DefaultNoopLifecycleHandler implements SparkApplicationLifecycleHandler {

    @Override
    public Mono<ApplicationCreateResult> onSparkAppCreate(YarnApplication request, Map<String, String> env, ServerRequest serverRequest) {
        String appId = "application_" + Instant.now().toEpochMilli() + "_" + Math.abs(UUID.randomUUID().getLeastSignificantBits() % 10000);
        return Mono.just(new ApplicationCreateResult(appId, "ACCEPTED", Map.of()));
    }

    @Override
    public Mono<NewApplication> onNewApplication(ServerRequest serverRequest) {
        NewApplication newApplication = new NewApplication();
        newApplication.setApplicationId("application_" + Instant.now().toEpochMilli() + "_0001");
        NewApplication.MaximumResourceCapability cap = new NewApplication.MaximumResourceCapability();
        cap.setMemory("16384");
        cap.setVCores("8");
        newApplication.setCapability(cap);
        return Mono.just(newApplication);
    }

    @Override
    public Mono<String> getApplicationState(String applicationId) {
        // Default no-op: always accepted
        return Mono.just("ACCEPTED");
    }

    @Override
    public Mono<Void> updateApplication(String applicationId, String desiredState) {
        // Default no-op: accept request
        return Mono.empty();
    }
}
