package varga.weave.rest;

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

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@Data
public class AmContainerSpec {
    @JsonProperty("environment")
    private Environment environment;
    @JsonProperty("local-resources")
    private LocalResources localResources;
    @JsonProperty("commands")
    private Commands commands;

    @Data
    public static class Environment {
        @JsonProperty("entry")
        private List<KeyStringValue> entries;
    }

    @Data
    public static class LocalResources {
        @JsonProperty("entry")
        private List<KeyFileValue> entries;
    }

    @Data
    public static class Commands {
        @JsonProperty("command")
        private String command;
    }

    @Data
    public static class KeyStringValue {
        private String key;
        private String value;
    }

    @Data
    public static class KeyFileValue {
        private String key;
        private FileValue value;
    }

    @Data
    public static class FileValue {
        private String resource;
        private long size;
        private long timestamp;
        private String type;
        private String visibility;
    }
}
