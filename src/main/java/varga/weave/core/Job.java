package varga.weave.core;



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
