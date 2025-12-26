package varga.weave.core;


import lombok.Data;

@Data
public class HadoopOptions extends Options {
    private String applicationType;
    private String applicationId;
    private String jobId;
    private String command;
    private String containerInstanceType;
}
