package varga.weave.rpc.protocol;

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

import com.google.protobuf.Message;
import com.google.protobuf.ServiceException;
import varga.weave.rpc.IdUtils;
import varga.weave.rpc.SocketContext;
import varga.weave.virtual.VirtualApplicationRepository;
import varga.weave.virtual.VirtualClusterRepository;
import varga.weave.job.port.*;
import varga.weave.core.Metadata;
import varga.weave.core.Tenant;
import varga.weave.core.date.InstantFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.*;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.*;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.unit.DataSize;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuple2;

import java.io.DataInputStream;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;

import static reactor.util.function.Tuples.of;

@Slf4j
@Service
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class YarnApplicationClientProtocolService {

    private final RpcAuthenticationManager rpcAuthenticationManager;
    private final VirtualApplicationRepository virtualApplicationRepository;
    private final VirtualClusterRepository virtualClusterRepository;
    private final BridgeManagerInputPort bridgeManagerInputPort;
    private final ProjectManagerInputPort projectManagerInputPort;
    private final JobManagerInputPort jobManagerInputPort;
    private final InfrastructureManagerInputPort infrastructureManagerInputPort;
    private final TenantManagerInputPort tenantManagerInputPort;
    private final DataManagerInputPort dataManagerInputPort;
    private final InstantFactory instantFactory;

    public Message getNewApplication(DataInputStream in,
                                     SocketContext socketContext) throws Exception {
//        rpc getNewApplication (GetNewApplicationRequestProto) returns (GetNewApplicationResponseProto);
        GetNewApplicationRequestProto req =
                GetNewApplicationRequestProto.parseDelimitedFrom(in);
        log.info("getNewApplication - request:\n{}", ToStringBuilder.reflectionToString(req));

        String tenantId = ProtocolUtils.getTenantId(req.getUnknownFields());
        String bridgeId = ProtocolUtils.getBridgeId(req.getUnknownFields());
        String projectId = ProtocolUtils.getProjectId(req.getUnknownFields()); // TODO use project
        String infrastructureId = ProtocolUtils.getInfrastructureId(req.getUnknownFields());

        return this.tenantManagerInputPort.findTenantById(tenantId)
                .switchIfEmpty(Mono.error(new ServiceException("Tenant " + tenantId + " not found")))
                .flatMap(tenant ->
                        this.infrastructureManagerInputPort.findCapacityByInfrastructureById(tenant, infrastructureId)
                                .flatMap(i -> this.virtualClusterRepository.getClusterId(tenant)
                                        .map(clusterId -> {
                                            // ApplicationId represents the globally unique identifier for an application.
                                            // The globally unique nature of the identifier is achieved by using the cluster timestamp i.e.
                                            // start-time of the ResourceManager along with a monotonically increasing counter for the application.
                                            int appId = Math.abs(new Random().nextInt(Integer.MAX_VALUE));
                                            ApplicationIdProto applicationId = ApplicationIdProto.newBuilder()
                                                    .setId(appId)
                                                    .setClusterTimestamp(clusterId)
                                                    .build();

                                            GetNewApplicationResponseProto response = GetNewApplicationResponseProto.newBuilder()
                                                    .setApplicationId(applicationId)
                                                    .setMaximumCapability(ResourceProto.newBuilder()
                                                            .setMemory((long) i.getTotalMemory().getMax().toMegabytes()) // GB internal to MB
                                                            .setVirtualCores((int) i.getTotalCpus().getMax())
                                                            .build())
                                                    .build();
                                            log.info("getNewApplication - response:\n{}", ToStringBuilder.reflectionToString(response));
                                            return response;
                                        })))
                .subscribeOn(Schedulers.boundedElastic())
                .block();
    }

    public Message getApplicationReport(DataInputStream in,
                                        SocketContext socketContext) throws Exception {
//        rpc getApplicationReport (GetApplicationReportRequestProto) returns (GetApplicationReportResponseProto);
        GetApplicationReportRequestProto req =
                GetApplicationReportRequestProto.parseDelimitedFrom(in);
        log.info("getApplicationReport - request:\n{}", ToStringBuilder.reflectionToString(req));

        ApplicationIdProto applicationId = req.getApplicationId();

        String tenantId = ProtocolUtils.getTenantId(req.getUnknownFields());
        String bridgeId = ProtocolUtils.getBridgeId(req.getUnknownFields());
        String projectId = ProtocolUtils.getProjectId(req.getUnknownFields());
        String infrastructureId = ProtocolUtils.getInfrastructureId(req.getUnknownFields());

        return this.tenantManagerInputPort.findTenantById(tenantId)
                .switchIfEmpty(Mono.error(new ServiceException("Tenant " + tenantId + " not found")))
                .flatMap(tenant ->
                        this.virtualApplicationRepository.getApplication(tenant, applicationId)
                                .switchIfEmpty(Mono.error(new ServiceException("Application " + IdUtils.getApplicationId(applicationId) + " not found")))
                                .flatMap(t4 -> this.jobManagerInputPort.findJobByJobId(tenant, t4.getT3())
                                        .switchIfEmpty(Mono.error(new ServiceException("Job " + t4.getT3() + " not found")))
                                        .flatMap(j -> this.jobManagerInputPort.findRunByJobIdAndRunId(tenant, t4.getT3(), t4.getT4())
                                                .map(run -> {
                                                    GetApplicationReportResponseProto response = prepareReport(applicationId, j, run);
                                                    log.info("getApplicationReport - response:\n{}", ToStringBuilder.reflectionToString(response));
                                                    return response;
                                                })
                                        )))
                .subscribeOn(Schedulers.boundedElastic())
                .block();
    }

    @NotNull
    private GetApplicationReportResponseProto prepareReport(ApplicationIdProto applicationId, Job j, JobRun run) {
        HadoopOptions options = (HadoopOptions) j.getOptions();

        GetApplicationReportResponseProto response = GetApplicationReportResponseProto.newBuilder()
                .setApplicationReport(getApplicationReport(applicationId, run, "hadoop"))
                .build();
        log.info("getApplicationReport - response:\n{}", ToStringBuilder.reflectionToString(response));
        return response;
    }

    @NotNull
    private static ApplicationReportProto getApplicationReport(ApplicationIdProto applicationId, JobRun run, String type) {
        YarnApplicationStateProto state;
        float progress = 0.0f;
        switch (run.getStatus()) {
            case "unknown":
                state = YarnApplicationStateProto.SUBMITTED;
                break;
            case "running":
                progress = 0.5f;
                state = YarnApplicationStateProto.RUNNING;
                break;
            case "complete":
                progress = 1.0f;
                state = YarnApplicationStateProto.FINISHED;
                break;
            case "failed":
                progress = 1.0f;
                state = YarnApplicationStateProto.FAILED;
                break;
            default:
                throw new RuntimeException("Unknown state " + run.getStatus());
        }
        return ApplicationReportProto.newBuilder()
                .setApplicationId(applicationId)
                .setApplicationType(type)
                .setUser("devil") // TODO
                .setQueue(Constants.QUEUE)
                .setStartTime(run.getCreated().getEpochSecond())
                .setProgress(progress)
                .setName(run.getName())
                .setTrackingUrl("https://ignored")
                .setYarnApplicationState(state)
                .build();
    }

    public Message getApplications(DataInputStream in,
                                   SocketContext socketContext) throws Exception {
//    rpc getApplications (GetApplicationsRequestProto) returns (GetApplicationsResponseProto);
        GetApplicationsRequestProto req =
                GetApplicationsRequestProto.parseDelimitedFrom(in);
        log.error("getApplications - request {}", ToStringBuilder.reflectionToString(req));

        String tenantId = ProtocolUtils.getTenantId(req.getUnknownFields());

        String infrastructureId = ProtocolUtils.getInfrastructureId(req.getUnknownFields());
        String bridgeId = ProtocolUtils.getBridgeId(req.getUnknownFields());
        String projectId = ProtocolUtils.getProjectId(req.getUnknownFields());

        return this.tenantManagerInputPort.findTenantById(tenantId)
                .switchIfEmpty(Mono.error(new ServiceException("Tenant " + tenantId + " not found")))
                .flatMap(tenant ->
                        this.rpcAuthenticationManager.withAuthentication(tenant, req,
                                this.projectManagerInputPort.findProjectById(tenant, projectId)
                                        .switchIfEmpty(Mono.error(new ServiceException("Project " + projectId + " not found")))
                                        .flatMap(project ->
                                                this.virtualClusterRepository.getClusterId(tenant)
                                                        .flatMap(clusterId ->
                                                                this.jobManagerInputPort.findLatestRunsByProject(tenant, project, 200, Optional.empty())
                                                                        .map(j -> {
                                                                            JobRun run = j.getRun();
                                                                            Job job = j.getJob();

                                                                            int appId = Math.abs(new Random().nextInt(Integer.MAX_VALUE));
                                                                            ApplicationIdProto applicationId = ApplicationIdProto.newBuilder()
                                                                                    .setId(appId)
                                                                                    .setClusterTimestamp(clusterId)
                                                                                    .build();

                                                                            return getApplicationReport(applicationId, run, "unknown");
                                                                        })
                                                                        .collectList()
                                                                        .map(r -> {
                                                                            GetApplicationsResponseProto response = GetApplicationsResponseProto.newBuilder()
                                                                                    .addAllApplications(r)
                                                                                    .build();
                                                                            log.error("getApplications - response:\n{}", ToStringBuilder.reflectionToString(req));
                                                                            return response;
                                                                        })))
                        ))
                .subscribeOn(Schedulers.boundedElastic())
                .block();
    }

    public Message submitApplication(DataInputStream in,
                                     SocketContext socketContext) throws Exception {
//        rpc submitApplication (SubmitApplicationRequestProto) returns (SubmitApplicationResponseProto);

        SubmitApplicationRequestProto req =
                SubmitApplicationRequestProto.parseDelimitedFrom(in);
        log.info("submitApplication - request:\n{}", ToStringBuilder.reflectionToString(req));

        ApplicationSubmissionContextProto yarnApplication = req.getApplicationSubmissionContext();
        ApplicationIdProto applicationId = yarnApplication.getApplicationId();

        String tenantId = ProtocolUtils.getTenantId(req.getUnknownFields());
        String bridgeId = ProtocolUtils.getBridgeId(req.getUnknownFields());
        String projectId = ProtocolUtils.getProjectId(req.getUnknownFields());
        String infrastructureId = ProtocolUtils.getInfrastructureId(req.getUnknownFields());

        Map<String, String> env = yarnApplication.getAmContainerSpec().getEnvironmentList().stream()
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));

        return this.tenantManagerInputPort.findTenantById(tenantId)
                .switchIfEmpty(Mono.error(new ServiceException("Tenant " + tenantId + " not found")))
                .flatMap(tenant -> this.bridgeManagerInputPort.findBridgeById(tenant, bridgeId)
                        .switchIfEmpty(Mono.error(new ServiceException("Bridge " + bridgeId + " not found")))
                        .flatMap(bridge ->
                                this.projectManagerInputPort.findProjectById(tenant, projectId)
                                        .switchIfEmpty(Mono.error(new ServiceException("Project " + projectId + " not found")))
                                        .flatMap(project -> {
                                            SubmitApplicationResponseProto response = SubmitApplicationResponseProto.newBuilder()
                                                    .build();
                                            log.info("submitApplication - response:\n{}", ToStringBuilder.reflectionToString(response));
                                            return createAndRun(socketContext, yarnApplication, applicationId, env, tenant, project, bridge, infrastructureId)
                                                    .thenReturn(response);
                                        })
                        )
                )
                .subscribeOn(Schedulers.boundedElastic())
                .block();
    }

    @NotNull
    private Mono<Void> createAndRun(SocketContext socketContext, ApplicationSubmissionContextProto
            yarnApplication, ApplicationIdProto applicationId, Map<String, String> env, Tenant tenant, Project project, Bridge bridge, String infrastructureId) {
        String endpoint = tenant.getMetadata().any().getOrDefault("api-endpoint-internal",
                tenant.getMetadata().any().get("api-endpoint")).toString();
        URI uri = URI.create(endpoint);

        // Prepare env
        env.putIfAbsent("JAVA_HOME", "/usr/local/openjdk-11");
        env.put("CONTAINER_ID", IdUtils.getContainerId(applicationId, null, null));
        env.put("JVM_PID", "$$");
        env.put("HADOOP_TOKEN_FILE_LOCATION", "/hadoop/yarn/local/appcache/tokens-file");
        env.put("LOCAL_DIRS", "/hadoop/yarn/local/filecache");
        // TODO debug hadoop
        env.put("HADOOP_JAAS_DEBUG", "true");
        env.put("APPLICATION_WEB_PROXY_BASE", "/bridges/" + bridge.getId() + "/" + IdUtils.getApplicationId(applicationId));

        env.put("NM_HOST", uri.getHost());
        env.put("NM_HTTP_PORT", String.valueOf(uri.getPort()));
        env.put("NM_PORT", "8081");

        env.put("APP_SUBMIT_TIME_ENV", String.valueOf(this.instantFactory.now().getEpochSecond()));
        String effectiveUser = socketContext.getEffectiveUser();
        env.put("USER", effectiveUser);

        env.put("HADOOP_HOME", "/usr/local/hadoop");
        env.put("HADOOP_PREFIX", "/usr/local/hadoop");
        env.put("HADOOP_COMMON_HOME", "/usr/local/hadoop");
        env.put("HADOOP_HDFS_HOME", "/usr/local/hadoop");
        env.put("HADOOP_MAPRED_HOME", "/usr/local/hadoop");
        env.put("HADOOP_YARN_HOME", "/usr/local/hadoop");
        env.put("HADOOP_CONF_DIR", "/etc/hadoop/conf");
        env.put("YARN_CONF_DIR", "/etc/hadoop/conf");

        env.computeIfPresent("CLASSPATH", (k, v) -> v + ":$HADOOP_COMMON_HOME/share/hadoop/tools/lib/*");

        Map<String, String> finalEnv = env.entrySet().stream().map(e -> of(e.getKey(), ContainerUtils.renderCommand(e.getValue()))).collect(Collectors.toMap(Tuple2::getT1, Tuple2::getT2));

        // Create and save a new job
        Job job = new Job();
        job.setName(yarnApplication.getApplicationName());
        job.setMaxRetries(Integer.valueOf(yarnApplication.getMaxAppAttempts()));
        job.setScope("user");
        job.setLabels(Map.of(
                        "varga.weave/x-hadoop-app-id", IdUtils.getApplicationId(applicationId),
                        "varga.weave/x-hadoop-job-id", IdUtils.getJobId(applicationId)
                )
        );
        job.setBridgeId(bridge.getId());
        Metadata metadata = new Metadata();
        metadata.set(Constants.HADOOP_USER, effectiveUser);
        job.setMetadata(metadata);

        HadoopOptions options = new HadoopOptions();
        job.setOptions(options);
        options.setApplicationType(yarnApplication.getApplicationType());
        options.setApplicationId(IdUtils.getApplicationId(applicationId));
        options.setJobId(IdUtils.getJobId(applicationId));
        options.setDockerImage(finalEnv.getOrDefault("WEAVE_HADOOP_IMAGE", "docker.io/vargafoundation/hadoop:latest"));
        prepareResource(yarnApplication, options);
        options.setCommand(yarnApplication.getAmContainerSpec().getCommandList().stream()
                // Clean the command
                .map(ContainerUtils::renderCommand)
                .collect(Collectors.joining(";")));
        options.setEnv(finalEnv);

        return this.jobManagerInputPort.createJob(tenant, project, job)
                // Create a run
                .flatMap(j -> {
                    JobRun run = new JobRun();
                    run.setName("TODO");
                    return
                            this.dataManagerInputPort.ensureFolder(tenant, "/tmp/hadoop-yarn/staging/history/done_intermediate/" + effectiveUser)
                                    .then(this.dataManagerInputPort.ensureFolder(tenant, "/tmp/hadoop-yarn/staging/" + effectiveUser + "/.staging"))
                                    .then(this.jobManagerInputPort.createRunForJob(tenant, j.getId(), run).map(r -> of(j, r)));
                })
                // Map the job into the table for application_id -> tenant_id,job_id,run_id mapping
                .flatMap(t -> this.virtualApplicationRepository.saveApplication(tenant, applicationId, infrastructureId, project.getId(), t.getT1().getId(), t.getT2().getId()))
                ;
    }

    private void prepareResource(ApplicationSubmissionContextProto yarnApplication, HadoopOptions options) {
        ResourceRequestProto amContainerResourceRequest = yarnApplication.getAmContainerResourceRequest(0);
        DataSize memory = DataSize.ofMegabytes(amContainerResourceRequest.getCapability().getMemory());
        int cpu = amContainerResourceRequest.getCapability().getVirtualCores();
        options.setInstanceType("Standard_General_G1_v1"); // TODO
    }

    public Message signalToContainer(DataInputStream in,
                                     SocketContext socketContext) throws Exception {
//    rpc signalToContainer(SignalContainerRequestProto) returns (SignalContainerResponseProto);
        SignalContainerRequestProto req =
                SignalContainerRequestProto.parseDelimitedFrom(in);
        log.error("signalToContainer - request:\n{}", ToStringBuilder.reflectionToString(req));

        // TODO

        SignalContainerResponseProto response = SignalContainerResponseProto.newBuilder().build();
        log.error("signalToContainer - response:\n{}", ToStringBuilder.reflectionToString(response));
        return response;
    }

    public Message getContainerReport(DataInputStream in,
                                      SocketContext socketContext) throws Exception {
//        rpc getContainerReport(GetContainerReportRequestProto) returns (GetContainerReportResponseProto);
        GetContainerReportRequestProto req =
                GetContainerReportRequestProto.parseDelimitedFrom(in);
        log.error("getContainerReport - request:\n{}", ToStringBuilder.reflectionToString(req));

        // TODO

        GetContainerReportResponseProto response = GetContainerReportResponseProto.newBuilder().build();
        log.error("getContainerReport - response:\n{}", ToStringBuilder.reflectionToString(response));
        return response;
    }

    public Message getContainers(DataInputStream in,
                                 SocketContext socketContext) throws Exception {
//    rpc getContainers(GetContainersRequestProto) returns (GetContainersResponseProto);
        GetContainersRequestProto req =
                GetContainersRequestProto.parseDelimitedFrom(in);
        log.error("getContainers - request:\n{}", ToStringBuilder.reflectionToString(req));

        // TODO

        GetContainersResponseProto response = GetContainersResponseProto.newBuilder()
                .build();
        log.error("getContainers - response:\n{}", ToStringBuilder.reflectionToString(response));
        return response;
    }
}
