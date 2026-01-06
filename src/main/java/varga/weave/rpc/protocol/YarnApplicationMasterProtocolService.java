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

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.ServiceException;
import varga.weave.rpc.IdUtils;
import varga.weave.rpc.SocketContext;
import varga.weave.virtual.VirtualApplicationRepository;
import varga.weave.virtual.VirtualClusterRepository;
import varga.weave.virtual.VirtualContainerRepository;
import varga.weave.job.k8s.KubernetesClientFactory;
import varga.weave.job.k8s.KubernetesUtils;
import varga.weave.job.port.InfrastructureManagerInputPort;
import varga.weave.job.port.JobManagerInputPort;
import varga.weave.job.port.TenantManagerInputPort;
import varga.weave.core.KubernetesTarget;
import varga.weave.yarn.rpc.protocol.Protos.ListStringStringMapProto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.proto.SecurityProtos.TokenProto;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.proto.YarnProtos.*;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.*;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.DataInputStream;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

// https://github.com/trisberg/yarn-examples/blob/master/simple-yarn-example/src/main/java/com/springdeveloper/demo/ApplicationMaster.java
@Slf4j
@Service
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class YarnApplicationMasterProtocolService {

    private final InfrastructureManagerInputPort infrastructureManagerInputPort;
    private final JobManagerInputPort jobManagerInputPort;
    private final TenantManagerInputPort tenantManagerInputPort;
    private final VirtualApplicationRepository virtualApplicationRepository;
    private final VirtualContainerRepository virtualContainerRepository;
    private final VirtualClusterRepository virtualClusterRepository;
    private final KubernetesClientFactory kubernetesClientFactory;

    public Message registerApplicationMaster(DataInputStream in,
                                             SocketContext socketContext) throws Exception {
//        rpc registerApplicationMaster (RegisterApplicationMasterRequestProto) returns (RegisterApplicationMasterResponseProto);
        RegisterApplicationMasterRequestProto req =
                RegisterApplicationMasterRequestProto.parseDelimitedFrom(in);
        log.info("registerApplicationMaster - request:\n{}", ToStringBuilder.reflectionToString(req));

        String tenantId = ProtocolUtils.getTenantId(req.getUnknownFields());
        String infrastructureId = ProtocolUtils.getInfrastructureId(req.getUnknownFields());
        String bridgeId = ProtocolUtils.getBridgeId(req.getUnknownFields());
        String projectId = ProtocolUtils.getProjectId(req.getUnknownFields());
        String jobId = ProtocolUtils.getJobId(req.getUnknownFields());
        String runId = ProtocolUtils.getRunId(req.getUnknownFields());

//        TokenIdentifier amrmTokenIdentifier =
//                YarnServerSecurityUtils.authorizeRequest();
//        ApplicationAttemptId applicationAttemptId =
//                amrmTokenIdentifier.getApplicationAttemptId();

        return this.tenantManagerInputPort.findTenantById(tenantId)
                .switchIfEmpty(Mono.error(new ServiceException("Tenant " + tenantId + " not found")))
                .flatMap(tenant ->
                        this.infrastructureManagerInputPort.findCapacityByInfrastructureById(tenant, infrastructureId)
                                .map(i -> {
                                    RegisterApplicationMasterResponseProto response = RegisterApplicationMasterResponseProto.newBuilder()
                                            .setQueue(Constants.QUEUE)
                                            .setClientToAmTokenMasterKey(ByteString.copyFromUtf8("randommmkey"))
                                            .setMaximumCapability(ResourceProto.newBuilder()
                                                    .setMemory(i.getTotalMemory().getMax().toMegabytes()) // GB internal to MB
                                                    .setVirtualCores((int) i.getTotalCpus().getMax())
                                                    .build())
//                                            .setClientToAmTokenMasterKey()
                                            .build();
                                    log.info("registerApplicationMaster - response:\n{}", ToStringBuilder.reflectionToString(response));
                                    return response;
                                }))
                .subscribeOn(Schedulers.boundedElastic())
                .block();
    }

    public Message finishApplicationMaster(DataInputStream in,
                                           SocketContext socketContext) throws Exception {
//        rpc finishApplicationMaster (FinishApplicationMasterRequestProto) returns (FinishApplicationMasterResponseProto);
        FinishApplicationMasterRequestProto req =
                FinishApplicationMasterRequestProto.parseDelimitedFrom(in);
        log.info("finishApplicationMaster - request:\n{}", ToStringBuilder.reflectionToString(req));

        String trackingUrl = req.getTrackingUrl();
        Long clusterId = IdUtils.getClusterIdFromTrackingUrl(trackingUrl);
        return this.virtualClusterRepository.getTenantId(clusterId)
                .flatMap(tenantId ->
                        this.tenantManagerInputPort.findTenantById(tenantId)
                                .switchIfEmpty(Mono.error(new ServiceException("Tenant " + tenantId + " not found")))
                                .flatMap(tenant -> {

                                    int applicationId = IdUtils.getApplicationIdFromTrackingUrl(trackingUrl);

                                    return this.virtualApplicationRepository.getApplication(tenant, ApplicationIdProto.newBuilder()
                                                    .setClusterTimestamp(clusterId)
                                                    .setId(applicationId)
                                                    .build())
                                            .flatMap(t4 -> {
                                                String infrastructureId = t4.getT1();
                                                String projectId = t4.getT2();
                                                String jobId = t4.getT3();
                                                String runId = t4.getT4();

                                                return this.infrastructureManagerInputPort.findInfrastructureById(tenant, infrastructureId)
                                                        .flatMap(infrastructure -> {
                                                            KubernetesTarget target = (KubernetesTarget) infrastructure.getTarget();
                                                            return this.kubernetesClientFactory.usingKubeClientWhen(target, kubernetesClient -> {

                                                                Map<String, String> labels = Map.of(
                                                                        KubernetesUtils.TENANT_ID, tenantId,
                                                                        KubernetesUtils.PROJECT_ID, projectId,
                                                                        KubernetesUtils.JOB_ID, jobId,
                                                                        KubernetesUtils.RUN_ID, runId
                                                                );

                                                                FinishApplicationMasterResponseProto response = FinishApplicationMasterResponseProto.newBuilder()
                                                                        .setIsUnregistered(true)
                                                                        .build();
                                                                log.info("finishApplicationMaster - response:\n{}", ToStringBuilder.reflectionToString(response));
                                                                return Mono.empty() // TODO for debugging, no deletion
//                                                            return this.kubernetesUtils.deleteResourcesWithLabels(kubernetesClient, target.getNamespace(), labels)
                                                                        .thenReturn(response);
                                                            });
                                                        });
                                            });
                                }))
                .subscribeOn(Schedulers.boundedElastic())
                .block();
    }

    public Message allocate(DataInputStream in,
                            SocketContext socketContext) throws Exception {
//        rpc allocate (AllocateRequestProto) returns (AllocateResponseProto);
        AllocateRequestProto req =
                AllocateRequestProto.parseDelimitedFrom(in);
        log.info("allocate - request:\n{}", ToStringBuilder.reflectionToString(req));

        if (!CollectionUtils.isEmpty(req.getAskList())) {
            return Flux.range(0, req.getAskList().stream().filter(a -> "*".equals(a.getResourceName())).findFirst().get().getNumContainers())
                    .flatMap(i -> {
                        ResourceRequestProto resourceRequest = req.getAskList().get(i);

                        String tenantId = ProtocolUtils.getTenantId(resourceRequest.getUnknownFields());
                        String bridgeId = ProtocolUtils.getBridgeId(resourceRequest.getUnknownFields());
                        String projectId = ProtocolUtils.getProjectId(resourceRequest.getUnknownFields());
                        String infrastructureId = ProtocolUtils.getInfrastructureId(resourceRequest.getUnknownFields());
                        String jobId = ProtocolUtils.getJobId(resourceRequest.getUnknownFields());
                        String runId = ProtocolUtils.getRunId(resourceRequest.getUnknownFields());

                        return this.tenantManagerInputPort.findTenantById(tenantId)
                                .switchIfEmpty(Mono.error(new ServiceException("Tenant " + tenantId + " not found")))
                                .flatMap(tenant -> this.virtualApplicationRepository.getApplication(tenant, infrastructureId, projectId, jobId, runId)
                                        .switchIfEmpty(Mono.error(new ServiceException("Application " + jobId + "/" + runId + " not found")))
                                        .flatMap(appId -> {

                                            String endpoint = tenant.getMetadata().any().getOrDefault("api-endpoint-internal",
                                                    tenant.getMetadata().any().get("api-endpoint")).toString();
                                            URI uri = URI.create(endpoint);

                                            // TODO get bridge

                                            NodeIdProto nodeId = NodeIdProto.newBuilder()
                                                    .setHost(uri.getHost())
                                                    .setPort(8081) // TODO
                                                    .build();

                                            // Increment for container
                                            return this.virtualContainerRepository.nextContainerId(tenant, appId)
                                                    .map(containerId -> ContainerProto.newBuilder()
                                                            .setId(containerId)
                                                            // .setResource(req.getSchedulingRequestsList().get(i).getResourceSizing().getResources())
                                                            .setResource(resourceRequest.getCapability())
                                                            .setPriority(resourceRequest.getPriority())
                                                            .setAllocationRequestId(resourceRequest.getAllocationRequestId())
                                                            .setExecutionType(resourceRequest.getExecutionTypeRequest().getExecutionType())
                                                            .setNodeId(nodeId)
                                                            .setNodeHttpAddress(uri.getHost() + ":" + uri.getPort())
                                                            .setContainerToken(TokenProto.newBuilder()
                                                                    .setKind("ContainerIdProto")
                                                                    .setService("allocate")
                                                                    .setIdentifier(containerId.toByteString())
                                                                    .setPassword(
                                                                            ListStringStringMapProto.newBuilder()
                                                                                    .addEntry(StringStringMapProto.newBuilder()
                                                                                            .setKey("tenant_id").setValue(tenantId))
                                                                                    .addEntry(StringStringMapProto.newBuilder()
                                                                                            .setKey("bridge_id").setValue(bridgeId))
                                                                                    .addEntry(StringStringMapProto.newBuilder()
                                                                                            .setKey("project_id").setValue(projectId))
                                                                                    .addEntry(StringStringMapProto.newBuilder()
                                                                                            .setKey("infrastructure_id").setValue(infrastructureId))
                                                                                    .addEntry(StringStringMapProto.newBuilder()
                                                                                            .setKey("job_id").setValue(jobId))
                                                                                    .addEntry(StringStringMapProto.newBuilder()
                                                                                            .setKey("run_id").setValue(runId))
                                                                                    .build().toByteString())
                                                                    .build())
                                                            //                .setAllocationRequestId()
                                                            .build());
                                        }));
                    })
                    .collect(Collectors.toList())
                    .flatMap(containers -> {

                        List<NMTokenProto> tokens = containers.stream().map(c -> NMTokenProto.newBuilder()
                                .setNodeId(c.getNodeId())
                                .setToken(TokenProto.newBuilder()
                                        .setKind("Ignored")
                                        .setService("Ignored")
                                        .setIdentifier(ByteString.copyFromUtf8("Ignored"))
                                        .setPassword(ByteString.EMPTY)
                                        .build())
                                .build()).collect(Collectors.toList());

                        AllocateResponseProto response = AllocateResponseProto.newBuilder()
                                .addAllAllocatedContainers(containers)
//                            .setCompletedContainerStatuses()
                                .setResponseId(req.getResponseId() + 1)
                                .setNumClusterNodes(1) // TODO static?
                                .addAllNmTokens(tokens)
                                .setAmRmToken(getAMRMToken())
                                .build();
                        log.info("allocate - response:\n{}", ToStringBuilder.reflectionToString(response));
                        return this.virtualContainerRepository.saveContainer(null, containers) // Register container for start // TODO tenant
                                .thenReturn(response);
                    })
                    .subscribeOn(Schedulers.boundedElastic())
                    .block();
        }

        // TODO failed containers
//        ContainerProto.newBuilder()
//                .setId(containerId)
//                .build()

        return AllocateResponseProto.newBuilder()
//                .addAllCompletedContainerStatuses()
                .setResponseId(req.getResponseId() + 1)
                .setNumClusterNodes(1) // TODO static?
                .build();
    }

    @NotNull
    private TokenProto getAMRMToken() {
        Token<AMRMTokenIdentifier> amrmToken = new Token<>("MyIdentifier".getBytes(), "MyPassword".getBytes(), AMRMTokenIdentifier.KIND_NAME, new Text("localhost:0"));
        TokenProto tokenProto = amrmToken.toTokenProto();

        TokenProto build = TokenProto.newBuilder()
                .setKind(tokenProto.getKind())
                .setService(tokenProto.getService())
                .setIdentifier(tokenProto.getIdentifier())
                .setPassword(tokenProto.getPassword())
                .build();
        return build;
    }

//    public Message allocate(DataInputStream in,
//                            SocketContext socketContext) throws Exception {
////        rpc allocate (AllocateRequestProto) returns (AllocateResponseProto);
//        AllocateRequestProto req =
//                AllocateRequestProto.parseDelimitedFrom(in);
//        log.info("allocate - request:\n{}", ToStringBuilder.reflectionToString(req));
//
//        String tenantId = ProtocolUtils.getTenantId(req.getUnknownFields());
//        String bridgeId = ProtocolUtils.getBridgeId(req.getUnknownFields());
//        String projectId = ProtocolUtils.getProjectId(req.getUnknownFields());
//        String jobId = ProtocolUtils.getJobId(req.getUnknownFields());
//        String runId = ProtocolUtils.getRunId(req.getUnknownFields());
//
//        return this.tenantManagerInputPort.findTenantById(tenantId)
//                .switchIfEmpty(Mono.error(new ServiceException("Tenant " + tenantId + " not found")))
//                .flatMap(tenant -> this.virtualApplicationRepository.getApplication(tenant, projectId, jobId, runId)
//                                .switchIfEmpty(Mono.error(new ServiceException("Application " + jobId + "/" + runId + " not found")))
//                                .flatMap(appId -> {
////                                    List<ContainerProto> containers = IntStream.range(0, req.getSchedulingRequestsList().size())
//                                    List<ContainerProto> containers = IntStream.range(0, req.getAskList().size())
//                                            .mapToObj(i -> {
//                                                ContainerIdProto containerId = ContainerIdProto.newBuilder()
//                                                        .setAppId(appId)
//                                                        .setAppAttemptId(ApplicationAttemptIdProto.newBuilder()
//                                                                .setApplicationId(appId)
//                                                                .setAttemptId(0)
//                                                                .build())
//                                                        .setId(i)
//                                                        .build();
//                                                ContainerProto containerProto = ContainerProto.newBuilder()
//                                                        .setId(containerId)
////                                                        .setResource(req.getSchedulingRequestsList().get(i).getResourceSizing().getResources())
//                                                        .setResource(req.getAskList().get(i).getCapability())
//                                                        .setNodeId(NodeIdProto.newBuilder()
//                                                                .setHost("virtual")
//                                                                .setPort(0)
//                                                                .build())
//                                                        .setExecutionType(ExecutionTypeProto.GUARANTEED)
//                                                        .setContainerToken(TokenProto.newBuilder()
//                                                                .setKind("ContainerIdProto")
//                                                                .setService("allocate")
//                                                                .setIdentifier(containerId.toByteString())
//                                                                .build())
////                .setAllocationRequestId()
//                                                        .build();
//                                                return containerProto;
//                                            }).collect(Collectors.toList());
//
//                                    return this.virtualContainerRepository.saveContainer(tenant, containers) // Register container for start
//                                            .thenReturn(AllocateResponseProto.newBuilder()
//                                                    .addAllAllocatedContainers(containers)
//                                                    .setResponseId(0)
//                                                    .build());
//                                })
//                )
//                .subscribeOn(Schedulers.boundedElastic())
//                .block();
//    }

}
