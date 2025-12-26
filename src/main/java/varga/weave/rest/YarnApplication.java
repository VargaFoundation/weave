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

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class YarnApplication {
    @JsonProperty("am-container-spec")
    private AmContainerSpec amContainerSpec;
    @JsonProperty("queue")
    private String queue;
    @JsonProperty("application-id")
    private String applicationId;
    @JsonProperty("application-name")
    private String applicationName;
    @JsonProperty("max-app-attempts")
    private String maxAppAttempts = "0";
    @JsonProperty("application-type")
    private String applicationType;

}
