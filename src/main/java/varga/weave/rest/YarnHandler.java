package varga.weave.rest;

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
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Mono;
import varga.weave.api.SparkApplicationLifecycleHandler;

import java.util.Map;
import java.util.stream.Collectors;

// https://github.com/bernhard-42/spark-yarn-rest-api/blob/master/spark-yarn.json.template
// https://tookitaki.gitbook.io/tdss-yarn-integration-guide/preqrequisites
// https://docs.cloudera.com/HDPDocuments/HDP2/HDP-2.6.0/bk_yarn-resource-management/content/ch_yarn_rest_apis.html
@Component
@RequiredArgsConstructor
public class YarnHandler {

    private final SparkApplicationLifecycleHandler lifecycleHandler;

    public Mono<ServerResponse> createApplication(ServerRequest request) {
        return request.bodyToMono(YarnApplication.class)
                .flatMap(yarnApplication -> {
                    if (yarnApplication.getAmContainerSpec() == null ||
                            yarnApplication.getAmContainerSpec().getEnvironment() == null) {
                        return ServerResponse.badRequest().bodyValue("Environment cannot be null");
                    }
                    Map<String, String> env = yarnApplication.getAmContainerSpec().getEnvironment().getEntries().stream()
                            .collect(Collectors.toMap(AmContainerSpec.KeyStringValue::getKey, AmContainerSpec.KeyStringValue::getValue));

                    return lifecycleHandler.onSparkAppCreate(yarnApplication, env, request)
                            .flatMap(result -> ServerResponse.accepted()
                                    .header("Location", request.uriBuilder().pathSegment(result.getApplicationId()).build().toString())
                                    .contentType(MediaType.APPLICATION_JSON)
                                    .bodyValue(result));
                });
    }

    // ex: "/usr/java/jdk1.7.0_71/bin/java -server -Xmx512m org.apache.spark.deploy.yarn.ApplicationMaster --jar __app__.jar --class org.apache.spark.examples.JavaWordCount --args hdfs://10.2.45.38:8020/demo.txt --args hdfs://10.2.45.38:8020/output 1>/tmp/sparkAppMaster.stdout 2>/tmp/sparkAppMaster.stderr"
    protected static String extractClassName(String command) {
        return "org.apache.spark.examples.JavaWordCount";
    }

    public Mono<ServerResponse> newApplication(ServerRequest request) {
        return this.lifecycleHandler.onNewApplication(request)
                .flatMap(m -> ServerResponse.ok().contentType(MediaType.APPLICATION_JSON).bodyValue(m));
    }

    public Mono<ServerResponse> getApplication(ServerRequest request) {
        // Not implemented: provide by user handler if needed
        return ServerResponse.noContent().build();
    }

    public Mono<ServerResponse> updateApplication(ServerRequest request) {
        String applicationId = request.pathVariable("application_id");
        return request.bodyToMono(Map.class)
                .flatMap(body -> this.lifecycleHandler.updateApplication(applicationId, String.valueOf(body.get("state"))))
                .then(ServerResponse.accepted().build());
    }

    public Mono<ServerResponse> getApplicationState(ServerRequest request) {
        String applicationId = request.pathVariable("application_id");
        return this.lifecycleHandler.getApplicationState(applicationId)
                .flatMap(state -> ServerResponse.ok().contentType(MediaType.APPLICATION_JSON).bodyValue(Map.of("state", state)));
    }
}

