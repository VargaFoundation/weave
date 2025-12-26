package varga.weave.rpc.protocol;

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

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.ServiceException;
import varga.weave.job.Infrastructure;
import varga.weave.job.k8s.KubernetesClientFactory;
import varga.weave.job.k8s.KubernetesUtils;
import varga.weave.job.k8s.NamespaceUtils;
import varga.weave.job.port.JobManagerInputPort;
import varga.weave.common.FreemarkerRenderer;
import varga.weave.core.KubernetesTarget;
import varga.weave.core.Utils;
import varga.weave.yarn.rpc.protocol.Protos;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.JobSpecBuilder;
import io.fabric8.kubernetes.client.utils.Serialization;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.proto.SecurityProtos.TokenProto;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.proto.YarnProtos.*;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import varga.weave.core.InfrastructureManagerInputPort;
import varga.weave.core.TenantManagerInputPort;
import varga.weave.rpc.IdUtils;
import varga.weave.rpc.SocketContext;
import varga.weave.virtual.VirtualApplicationRepository;
import varga.weave.virtual.VirtualClusterRepository;
import varga.weave.virtual.VirtualContainerRepository;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

import static varga.weave.rpc.protocol.Constants.APPLICATION_MASTER_HOSTNAME;

@Slf4j
@Service
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class YarnContainerManagementProtocolService {

    private final VirtualContainerRepository virtualContainerRepository;
    private final VirtualClusterRepository virtualClusterRepository;
    private final VirtualApplicationRepository virtualApplicationRepository;
    private final TenantManagerInputPort tenantManagerInputPort;
    private final JobManagerInputPort jobManagerInputPort;
    private final InfrastructureManagerInputPort infrastructureManagerInputPort;
    private final KubernetesClientFactory kubernetesClientFactory;
    private final FreemarkerRenderer freemarkerRenderer;
    private final NamespaceUtils namespaceUtils;

    public Message startContainers(DataInputStream in,
                                   SocketContext socketContext) throws Exception {
//        rpc startContainers(StartContainersRequestProto) returns (StartContainersResponseProto);
        StartContainersRequestProto req =
                StartContainersRequestProto.parseDelimitedFrom(in);
        log.info("startContainers - request:\n{}", ToStringBuilder.reflectionToString(req));

        return Flux.fromIterable(req.getStartContainerRequestList())
                .flatMap(startContainerRequestProto -> {
                    TokenProto containerToken = startContainerRequestProto.getContainerToken();
                    ContainerIdProto containerId;
                    try {
                        containerId = ContainerIdProto.parseFrom(containerToken.getIdentifier());
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException(e);
                    }

                    Protos.ListStringStringMapProto password;
                    try {
                        password = Protos.ListStringStringMapProto.parseFrom(containerToken.getPassword());
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException(e);
                    }

                    String tenantId = password.getEntryList().stream().filter(e -> "tenant_id".equals(e.getKey())).findFirst().get().getValue();
                    String bridgeId = password.getEntryList().stream().filter(e -> "bridge_id".equals(e.getKey())).findFirst().get().getValue();
                    String infrastructureId = password.getEntryList().stream().filter(e -> "infrastructure_id".equals(e.getKey())).findFirst().get().getValue();
                    String projectId = password.getEntryList().stream().filter(e -> "project_id".equals(e.getKey())).findFirst().get().getValue();
                    String jobId = password.getEntryList().stream().filter(e -> "job_id".equals(e.getKey())).findFirst().get().getValue();
                    String runId = password.getEntryList().stream().filter(e -> "run_id".equals(e.getKey())).findFirst().get().getValue();

                    return
                            this.tenantManagerInputPort.findTenantById(tenantId)
                                    .switchIfEmpty(Mono.error(new ServiceException("Tenant " + tenantId + " not found")))
                                    .flatMap(tenant ->
                                            this.jobManagerInputPort.findJobByJobId(tenant, jobId)
                                                    .flatMap(job ->
                                                            this.infrastructureManagerInputPort.findInfrastructureById(tenant, infrastructureId)
                                                                    .flatMap(infrastructure ->
                                                                            this.virtualContainerRepository.getAndRemoveContainer(tenant, IdUtils.getContainerId(containerId))
                                                                                    .flatMap(c ->
                                                                                            prepareJob(
                                                                                                    tenant,
                                                                                                    infrastructure,
                                                                                                    c,
                                                                                                    bridgeId,
                                                                                                    projectId,
                                                                                                    job,
                                                                                                    runId,
                                                                                                    IdUtils.getApplicationId(containerId.getAppId()),
                                                                                                    startContainerRequestProto.getContainerLaunchContext()
                                                                                            )
                                                                                    )
                                                                    )
                                                    )
                                                    .thenReturn(containerId));
                }).collectList()
                .map(ids -> StartContainersResponseProto.newBuilder()
                        .addAllSucceededRequests(ids)
                        .addServicesMetaData(StringBytesMapProto.newBuilder()
                                .setKey("mapreduce_shuffle")
                                .setValue(ByteString.copyFrom(ByteBuffer.allocate(4).putInt(8080).array()))
                                .build())
                        .build())
                .subscribeOn(Schedulers.boundedElastic())
                .block();
    }

    private Mono<Boolean> prepareJob(Tenant tenant, Infrastructure infrastructure, ContainerProto container, String bridgeId, String projectId, varga.weave.job.Job job, String runId, String applicationId, ContainerLaunchContextProto containerLaunchContext) {
        KubernetesTarget target = (KubernetesTarget) infrastructure.getTarget();
        return this.kubernetesClientFactory.usingKubeClient(target, kubernetesClient -> {
            String namespace = this.namespaceUtils.getOrCreateNamespace(kubernetesClient, tenant);

            String key = Utils.sig40(job.getId(), runId);
            Job jobApplicationMaster = kubernetesClient.batch().jobs().inNamespace(namespace).withName(key + "-job").get();
            Map<String, String> labels = jobApplicationMaster.getSpec().getTemplate().getMetadata().getLabels();
            Pod applicationMaster = kubernetesClient.pods().inNamespace(namespace).withLabels(labels).list().getItems().get(0);

            String containerId = IdUtils.getContainerId(container.getId());
            String mrJobId = IdUtils.getJobId(container.getId().getAppId());
            String containerIdForK8s = containerId.replace("_", "-");

            Map<String, String> env = containerLaunchContext.getEnvironmentList().stream()
                    .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));

            String effectiveUser = (String) job.getMetadata().any().get(Constants.HADOOP_USER);

            env.put("HADOOP_TOKEN_FILE_LOCATION", "/hadoop/yarn/local/appcache/tokens-file");
            env.putIfAbsent("JAVA_HOME", "/usr/local/openjdk-11");
            env.put("CONTAINER_ID", containerId);
            env.put("JVM_PID", "$$");
            // TODO debug hadoop
            env.put("HADOOP_JAAS_DEBUG", "true");
            env.put("USER", effectiveUser);
            env.put("LOCAL_DIRS", "/hadoop/yarn/local/filecache");
            env.put("HADOOP_HOME", "/usr/local/hadoop");
            env.put("HADOOP_PREFIX", "/usr/local/hadoop");
            env.put("HADOOP_COMMON_HOME", "/usr/local/hadoop");
            env.put("HADOOP_HDFS_HOME", "/usr/local/hadoop");
            env.put("HADOOP_MAPRED_HOME", "/usr/local/hadoop");
            env.put("HADOOP_YARN_HOME", "/usr/local/hadoop");
            env.put("HADOOP_CONF_DIR", "/etc/hadoop/conf");
            env.put("YARN_CONF_DIR", "/etc/hadoop/conf");
            env.computeIfPresent("CLASSPATH", (k, v) -> v + ":$HADOOP_COMMON_HOME/share/hadoop/tools/lib/*");

            String configMapName = containerIdForK8s + "-run-configmap";

            List<EnvVar> envVars = env.entrySet().stream()
                    .map(e -> new EnvVarBuilder()
                            .withName(e.getKey())
                            .withValue(ContainerUtils.renderCommand(e.getValue()))
                            .build()
                    ).collect(Collectors.toList());

            // Create tokens-file
            Credentials credentials = getCredentials(target, job, applicationMaster.getStatus().getPodIP());
            ByteArrayOutputStream serializedCredentials = new ByteArrayOutputStream();
            try {
                credentials.writeTokenStorageToStream(new DataOutputStream(serializedCredentials), Credentials.SerializedFormat.WRITABLE);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            byte[] res = serializedCredentials.toByteArray();

            ConfigMap hadoopTokenFileLocation = new ConfigMapBuilder()
                    .withNewMetadata()
                    .withNamespace(namespace)
                    .withName(containerIdForK8s + "-tokens-file-configmap")
                    .addToLabels(KubernetesUtils.TENANT_ID, tenant.getId())
                    .addToLabels(KubernetesUtils.PROJECT_ID, job.getProjectId())
                    .addToLabels(KubernetesUtils.JOB_ID, job.getId())
                    .addToLabels(KubernetesUtils.RUN_ID, runId)
                    .endMetadata()
                    .addToBinaryData("tokens", Base64.getEncoder().encodeToString(res))
                    .build();
            kubernetesClient.configMaps().create(hadoopTokenFileLocation);

            // Create run.sh
            Map<String, Object> root = new HashMap<>();
            root.put("tenant", tenant);
            root.put("target", target);
            root.put("infrastructureOrDevice", infrastructure);
            root.put("run", runId);
            root.put("job", job.getId());
            root.put("project", projectId);
            root.put("key", configMapName);
            root.put("env", envVars);
            root.put("commands", containerLaunchContext.getCommandList().stream()
                    // Clean the command
                    .map(ContainerUtils::renderCommand)
                    .map(c -> ContainerUtils.changeIp(applicationMaster.getStatus().getPodIP(), c))
//                    .map(c -> ContainerUtils.changeIp(APPLICATION_MASTER_HOSTNAME, c))
                    .collect(Collectors.toList()));
            ConfigMap runSh = kubernetesClient.configMaps().inNamespace(namespace)
                    .load(this.freemarkerRenderer.renderFromTemplate("yarn-container.run.sh.yaml.ftl", root))
                    .get();
            kubernetesClient.configMaps().create(runSh);

            Container initContainer = prepareInitContainer(tenant, effectiveUser, mrJobId, job);

            // Create job
            Job k8sJob = createJob(tenant, container, projectId, runId, applicationId, containerLaunchContext, namespace, key, applicationMaster, mrJobId, containerIdForK8s, configMapName, envVars, initContainer);

            // UGLY AF, but default mode on configmap is fucked
            String yaml = Serialization.asYaml(k8sJob);
            kubernetesClient.batch().jobs().inNamespace(namespace)
                    .load(new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8))).create();
            return true;
        });

    }

    private Credentials getCredentials(KubernetesTarget target, varga.weave.job.Job job, String applicationMasterIp) {
        String jobId = job.getLabels().get("varga.weave/x-hadoop-job-id");

        SecurityUtil.setTokenServiceUseIp(false);
        JobTokenSecretManager jobTokenSecretManager = new JobTokenSecretManager();
        JobTokenIdentifier tokenId = new JobTokenIdentifier(new Text(jobId));
        Token<JobTokenIdentifier> jobToken = new Token<>(tokenId, jobTokenSecretManager);
        jobTokenSecretManager.addTokenForJob(jobId, jobToken);

        SecurityUtil.setTokenService(jobToken, InetSocketAddress.createUnresolved(applicationMasterIp, 50020));

        log.info("Service address for token is " + jobToken.getService());

        Credentials credentials = new Credentials();
        credentials.addToken(jobToken.getService(), jobToken);
        TokenCache.setJobToken(jobToken, credentials);

        Text mapReduceShuffleToken = new Text("MapReduceShuffleToken");
        credentials.addSecretKey(mapReduceShuffleToken, jobToken.getPassword());

        log.info("Executing container with tokens: {}", credentials.getAllTokens());
        return credentials;
    }

    private Job createJob(Tenant tenant, ContainerProto container, String projectId, String runId, String applicationId, ContainerLaunchContextProto containerLaunchContext, String namespace, String key, Pod applicationMaster, String mrJobId, String containerIdForK8s, String configMapName, List<EnvVar> envVars, Container initContainer) {
        Job job = new JobBuilder()
                .withMetadata(new ObjectMetaBuilder()
                        .withName(containerIdForK8s)
                        .withLabels(Map.of(
                                KubernetesUtils.TENANT_ID, tenant.getId(),
                                KubernetesUtils.PROJECT_ID, projectId,
                                KubernetesUtils.JOB_ID, mrJobId,
                                KubernetesUtils.RUN_ID, runId,
                                "varga.weave/x-hadoop-app-id", applicationId
                        ))
                        .withNamespace(namespace)
                        .withOwnerReferences(new OwnerReferenceBuilder() // Set application master as owner
                                .withApiVersion(applicationMaster.getApiVersion())
                                .withName(applicationMaster.getMetadata().getName())
                                .withKind(applicationMaster.getKind())
                                .withUid(applicationMaster.getMetadata().getUid())
                                .build())
                        .build())
                .withSpec(new JobSpecBuilder()
                        .withParallelism(1)
                        .withCompletions(1)
                        .withBackoffLimit(containerLaunchContext.getContainerRetryContext().getMaxRetries())
                        .withTtlSecondsAfterFinished(3600)
                        .withTemplate(new PodTemplateSpecBuilder()
                                .withSpec(new PodSpecBuilder()
                                        .withRestartPolicy("Never")
                                        .withInitContainers(initContainer)
                                        .addToHostAliases(
                                                new HostAliasBuilder()
                                                        .addToHostnames(
                                                                APPLICATION_MASTER_HOSTNAME
                                                        )
                                                        .withIp(applicationMaster.getStatus().getPodIP())
                                                        .build()
                                        )
                                        .withVolumes(
                                                new VolumeBuilder()
                                                        .withName("run-command-volume")
                                                        .withConfigMap(new ConfigMapVolumeSourceBuilder()
                                                                .withName(configMapName)
                                                                .withDefaultMode(493) // 493 in binary -> 755 in decimal
                                                                .build())
                                                        .build(),
                                                new VolumeBuilder()
                                                        .withName("container-log4j-volume")
                                                        .withConfigMap(new ConfigMapVolumeSourceBuilder()
                                                                .withName(key + "-log4j-configmap")
                                                                .withDefaultMode(493) // 493 in binary -> 755 in decimal
                                                                .build())
                                                        .build(),
                                                new VolumeBuilder()
                                                        .withName("tokens-file-volume")
                                                        .withConfigMap(new ConfigMapVolumeSourceBuilder()
                                                                .withName(containerIdForK8s + "-tokens-file-configmap")
                                                                .withDefaultMode(493) // 493 in binary -> 755 in decimal
                                                                .build())
                                                        .build(),
                                                // Hadoop
                                                new VolumeBuilder()
                                                        .withName("core-site-volume")
                                                        .withConfigMap(new ConfigMapVolumeSourceBuilder()
                                                                .withName(key + "-core-site-xml")
                                                                .withDefaultMode(493) // 493 in binary -> 755 in decimal
                                                                .build())
                                                        .build(),
                                                new VolumeBuilder()
                                                        .withName("yarn-site-volume")
                                                        .withConfigMap(new ConfigMapVolumeSourceBuilder()
                                                                .withName(key + "-yarn-site-xml")
                                                                .withDefaultMode(493) // 493 in binary -> 755 in decimal
                                                                .build())
                                                        .build(),
                                                new VolumeBuilder()
                                                        .withName("mapred-site-volume")
                                                        .withConfigMap(new ConfigMapVolumeSourceBuilder()
                                                                .withName(key + "-mapred-site-xml")
                                                                .withDefaultMode(493) // 493 in binary -> 755 in decimal
                                                                .build())
                                                        .build(),
                                                // Temp Hadoop
                                                new VolumeBuilder()
                                                        .withName("hadoop-filecache-volume")
                                                        .withEmptyDir(new EmptyDirVolumeSourceBuilder()
                                                                .build())
                                                        .build()
                                        )
                                        .addToContainers(
                                                new ContainerBuilder()
                                                        .withName("container")
                                                        .withVolumeMounts(
                                                                new VolumeMountBuilder()
                                                                        .withName("run-command-volume")
                                                                        .withMountPath("/bin/run.sh")
                                                                        .withSubPath("run.sh")
                                                                        .withReadOnly(true)
                                                                        .build(),
                                                                // Temp Hadoop
                                                                new VolumeMountBuilder()
                                                                        .withName("hadoop-filecache-volume")
                                                                        .withMountPath("/hadoop/yarn/local/filecache")
                                                                        .build(),
                                                                new VolumeMountBuilder()
                                                                        .withName("container-log4j-volume")
                                                                        .withMountPath("/hadoop/yarn/local/appcache/container-log4j.properties")
                                                                        .withSubPath("container-log4j.properties")
                                                                        .withReadOnly(true)
                                                                        .build(),
                                                                new VolumeMountBuilder()
                                                                        .withName("container-log4j-volume")
                                                                        .withMountPath("/hadoop/yarn/local/appcache/logging.properties")
                                                                        .withSubPath("logging.properties")
                                                                        .withReadOnly(true)
                                                                        .build(),
                                                                new VolumeMountBuilder()
                                                                        .withName("tokens-file-volume")
                                                                        .withMountPath("/hadoop/yarn/local/appcache/tokens-file")
                                                                        .withSubPath("tokens")
                                                                        .withReadOnly(true)
                                                                        .build(),
                                                                // Hadoop
                                                                new VolumeMountBuilder()
                                                                        .withName("core-site-volume")
                                                                        .withMountPath("/etc/hadoop/conf/core-site.xml")
                                                                        .withSubPath("core-site.xml")
                                                                        .withReadOnly(true)
                                                                        .build(),
                                                                new VolumeMountBuilder()
                                                                        .withName("yarn-site-volume")
                                                                        .withMountPath("/etc/hadoop/conf/yarn-site.xml")
                                                                        .withSubPath("yarn-site.xml")
                                                                        .withReadOnly(true)
                                                                        .build(),
                                                                new VolumeMountBuilder()
                                                                        .withName("mapred-site-volume")
                                                                        .withMountPath("/etc/hadoop/conf/mapred-site.xml")
                                                                        .withSubPath("mapred-site.xml")
                                                                        .withReadOnly(true)
                                                                        .build()
                                                        )
                                                        .withImage("docker.io/vargafoundation/hadoop:latest") // TODO variable
                                                        .withEnv(
                                                                envVars
                                                        )
                                                        .withResources(new ResourceRequirementsBuilder()
                                                                .withLimits(Map.of(
                                                                        "memory", new Quantity(container.getResource().getMemory() + "M"),
                                                                        "cpu", new Quantity(container.getResource().getVirtualCores() + "")
                                                                ))
                                                                .build())
                                                        .withArgs("/bin/run.sh")
                                                        .build())
                                        .build())
                                .build())
                        .build())
                .build();
        return job;
    }

    private Container prepareInitContainer(Tenant tenant, String effectiveUser, String mrJobId, varga.weave.job.Job job) {
        String accessKeyId = tenant.getMetadata().any().get("datalake-blob-storage-access-id").toString();
        String accessKeySecret = tenant.getMetadata().any().get("datalake-blob-storage-access-secret").toString();
        String region = tenant.getMetadata().any().get("datalake-blob-storage-region").toString();
        String bucket = tenant.getMetadata().any().get("datalake-blob-storage-bucket").toString();
        String endpoint = tenant.getMetadata().any().getOrDefault("datalake-blob-storage-endpoint-internal",
                        tenant.getMetadata().any().getOrDefault("datalake-blob-storage-endpoint", "").toString())
                .toString();
//                                Boolean pathStyleAccess = Boolean.valueOf(tenant.getMetadata().any().getOrDefault("datalake-blob-storage-path-style-access", "false").toString()); // TODO

        Container initContainer = new ContainerBuilder()
                .withName("fetch-job-xml")
                .withEnv(
                        new EnvVarBuilder()
                                .withName("AWS_ACCESS_KEY_ID")
                                .withValue(accessKeyId)
                                .build(),
                        new EnvVarBuilder()
                                .withName("AWS_SECRET_ACCESS_KEY")
                                .withValue(accessKeySecret)
                                .build(),
                        new EnvVarBuilder()
                                .withName("AWS_DEFAULT_REGION")
                                .withValue(region)
                                .build()
                )
                .withImage("amazon/aws-cli:2.7.32")
                .withVolumeMounts(
                        // Temp Hadoop
                        new VolumeMountBuilder()
                                .withName("hadoop-filecache-volume")
                                .withMountPath("/hadoop/yarn/local/filecache")
                                .build())
                .withCommand(
                        "/bin/sh",
                        "-c",
                        renderCommand(this.freemarkerRenderer, effectiveUser, mrJobId, bucket, endpoint)
                )
                .build();

        return initContainer;
    }

    protected static String renderCommand(FreemarkerRenderer freemarkerRenderer, String effectiveUser, String mrJobId, String bucket, String endpoint) {
        return freemarkerRenderer.renderFromString(
                """
                        /bin/sh <<'EOF'
                        touch /hadoop/yarn/local/filecache/.keep
                        aws s3 ls s3://${bucket}/tmp/hadoop-yarn/staging/${effectiveUser}/.staging/${mrJobId}/ --endpoint-url ${endpoint} --no-verify-ssl --output json --debug
                        aws s3 cp s3://${bucket}/tmp/hadoop-yarn/staging/${effectiveUser}/.staging/${mrJobId}/job.xml /hadoop/yarn/local/filecache/job.xml --endpoint-url ${endpoint} --no-verify-ssl --debug
                        aws s3 cp s3://${bucket}/tmp/hadoop-yarn/staging/${effectiveUser}/.staging/${mrJobId}/job.split /hadoop/yarn/local/filecache/job.split --endpoint-url ${endpoint} --no-verify-ssl --debug
                        aws s3 cp s3://${bucket}/tmp/hadoop-yarn/staging/${effectiveUser}/.staging/${mrJobId}/job.splitmetainfo /hadoop/yarn/local/filecache/job.splitmetainfo --endpoint-url ${endpoint} --no-verify-ssl --debug
                        echo '* listing /hadoop/yarn/local/filecache/'
                        ls -all /hadoop/yarn/local/filecache/
                        EOF
                        """
                , new HashMap<>(Map.of(
                        "bucket", bucket,
                        "effectiveUser", effectiveUser,
                        "mrJobId", mrJobId,
                        "endpoint", endpoint
                ))
        );
    }

    public Message stopContainers(DataInputStream in,
                                  SocketContext socketContext) throws Exception {
//    rpc stopContainers(StopContainersRequestProto) returns (StopContainersResponseProto);
        StopContainersRequestProto req =
                StopContainersRequestProto.parseDelimitedFrom(in);
        log.info("stopContainers - request:\n{}", ToStringBuilder.reflectionToString(req));

        return Flux.fromIterable(req.getContainerIdList())
                .flatMap(id -> {
                    long clusterId = id.getAppId().getClusterTimestamp();
                    int applicationId = id.getAppId().getId();
                    return this.virtualClusterRepository.getTenantId(clusterId)
                            .flatMap(tenantId ->
                                    this.tenantManagerInputPort.findTenantById(tenantId)
                                            .switchIfEmpty(Mono.error(new ServiceException("Tenant " + tenantId + " not found")))
                                            .flatMap(tenant -> {
                                                ApplicationIdProto applicationId1 = ApplicationIdProto.newBuilder()
                                                        .setClusterTimestamp(clusterId)
                                                        .setId(applicationId)
                                                        .build();
                                                return this.virtualApplicationRepository.getApplication(tenant, applicationId1)
                                                        .flatMap(t4 -> {

                                                            String infrastructureId = t4.getT1();

                                                            return this.infrastructureManagerInputPort.findInfrastructureById(tenant, infrastructureId)
                                                                    .flatMap(infrastructure -> {

                                                                        KubernetesTarget target = (KubernetesTarget) infrastructure.getTarget();
                                                                        return this.kubernetesClientFactory.usingKubeClient(target, kubernetesClient -> {
                                                                            List<ContainerIdProto> succeededRequests = new ArrayList<>();
                                                                            List<ContainerExceptionMapProto> failedRequests = new ArrayList<>();
                                                                            String containerIdForK8s = IdUtils.getContainerId(id);
                                                                            try {
                                                                                // TODO for debugging, no deletion
//                                                                            kubernetesClient.batch().jobs().inNamespace(target.getNamespace())
//                                                                                    .withName(containerIdForK8s).delete();
                                                                                succeededRequests.add(id);
                                                                            } catch (Exception e) {
                                                                                log.error("Can not stop container {}", containerIdForK8s, e);
                                                                                failedRequests.add(ContainerExceptionMapProto.newBuilder()
                                                                                        .setContainerId(id)
                                                                                        .setException(SerializedExceptionProto.newBuilder()
                                                                                                .setMessage(e.getMessage())
                                                                                                .build())
                                                                                        .build());
                                                                            }

                                                                            return StopContainersResponseProto.newBuilder()
                                                                                    .addAllSucceededRequests(succeededRequests)
                                                                                    .addAllFailedRequests(failedRequests)
                                                                                    .build();
                                                                        });
                                                                    });
                                                        })
                                                        .flatMap(r -> this.virtualContainerRepository.getAndRemoveContainer(tenant, IdUtils.getContainerId(id))
                                                                .thenReturn(r)
                                                        );
                                            }));
                })
                .collectList()
                .map(r -> {
                    StopContainersResponseProto response = r.stream().reduce((a1, a2) -> StopContainersResponseProto.newBuilder()
                            .addAllSucceededRequests(a1.getSucceededRequestsList())
                            .addAllSucceededRequests(a2.getSucceededRequestsList())
                            .addAllFailedRequests(a1.getFailedRequestsList())
                            .addAllFailedRequests(a2.getFailedRequestsList())
                            .build()).get();
                    log.info("stopContainers - response:\n{}", ToStringBuilder.reflectionToString(response));
                    return response;
                })
                .subscribeOn(Schedulers.boundedElastic())
                .block();
    }
}
