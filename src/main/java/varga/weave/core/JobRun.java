package varga.weave.core;

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
