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
import java.util.List;

/**
 * An instance of a run.
 */
@Data
public class JobRun implements Serializable {
    /**
     * A incremental unique identifier.
     */
    private String id;

    private String jobId;

    private String projectId;

    private String infrastructureId;

    private String deviceId;
    /**
     * The run name.
     */
    private String name;
    /**
     * A description.
     */
    private String description;

    private String owner;
//
    private boolean isContinuous;
    /**
     * Represent the instant of the creation of this run: variables resolution, check configuration, prepare token and secret
     */
    private Instant created;
    /**
     * Represent the instant when the run has been updated.
     */
    private Instant updated;
    /**
     * The current status of the run.
     * Can not be null.
     */
    private String status;
    /**
     * A raw bucket containing all the specific properties of the run.
     */
    private Metadata metadata;

    private List<String> parameters;

}
