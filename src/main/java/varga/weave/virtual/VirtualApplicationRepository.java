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


import varga.weave.core.ApplicationRepository;
import com.google.protobuf.UnknownFieldSet;
import varga.weave.core.Tenant;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple4;
import reactor.util.function.Tuples;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service
public class VirtualApplicationRepository implements ApplicationRepository { // TODO use persistent storage

    private Map<ApplicationIdProto, Tuple4<String, String, String, String>> map = new HashMap<>();
    private Map<Tuple4<String, String, String, String>, ApplicationIdProto> reverseMap = new HashMap<>();

    public Mono<Void> saveApplication(Tenant tenant, ApplicationIdProto applicationId, String infrastructureId, String projectId, String jobId, String runId) {
        return Mono.fromCallable(() -> {
            // Skip unknown fields for hashcode
            ApplicationIdProto build = applicationId.toBuilder().setUnknownFields(UnknownFieldSet.newBuilder().build()).build();
            this.map.put(build, Tuples.of(infrastructureId, projectId, jobId, runId)); // TODO use tenant in map
            this.reverseMap.put(Tuples.of(infrastructureId, projectId, jobId, runId), build);
            return null;
        });
    }

    public Mono<Tuple4<String, String, String, String>> getApplication(Tenant tenant, ApplicationIdProto applicationId) {
        // Skip unknown fields for hashcode
        ApplicationIdProto build = applicationId.toBuilder().setUnknownFields(UnknownFieldSet.newBuilder().build()).build();
        return Mono.justOrEmpty(this.map.get(build));
    }

    public Mono<ApplicationIdProto> getApplication(Tenant tenant, String infrastructureId, String projectId, String jobId, String runId) {
        return Mono.justOrEmpty(this.reverseMap.get(Tuples.of(infrastructureId, projectId, jobId, runId)));
    }
}

