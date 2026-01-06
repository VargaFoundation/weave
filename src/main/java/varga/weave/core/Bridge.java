package varga.weave.core;

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

import lombok.Data;

import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@Data
public class Bridge implements Serializable {
    /**
     * A incremental unique identifier.
     */
    private String id;
    /**
     * The bridge type.
     */
    private String type;
    /**
     * A description.
     */
    private String description;

    private String owner;
    /**
     * Represent the instant of the creation of this bridge.
     */
    private Instant created;
    /**
     * Represent the instant when the bridge has been updated.
     */
    private Instant updated;

    private Map<String, String> configuration = new HashMap<>();
    /**
     * A raw bucket containing all the specific properties of the bridge.
     */
    private Metadata metadata;


}
