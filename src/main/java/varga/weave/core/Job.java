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
import java.util.List;
import java.util.Map;

@Data
public class Job implements Serializable {

    /**
     * A incremental unique identifier.
     */
    private String id;

    private String projectId;

    private String bridgeId;
    /**
     * The job name.
     */
    private String name;
    /**
     * The scope: user or system.
     */
    private String scope;
    /**
     * A description.
     */
    private String description;

    /**
     * Represent the instant of the creation of this job.
     */
    private Instant created;
    /**
     * Represent the instant when the job has been updated.
     */
    private Instant updated;

    private Options options;

    private List<Library> libraries;

    private List<String> secrets;
    /**
     * A raw bucket containing all the specific properties of the job.
     */
    private Metadata metadata;

    private List<String> parameters;

    private Map<String, String> labels = new HashMap<>();

    private Notifications notifications;

    @Transient
    public boolean isNew() {
        return true;
    }

}
