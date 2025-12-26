package varga.weave;

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

import varga.weave.job.*;
import varga.weave.job.api.common.TenantWebArgumentResolver;
import varga.weave.job.port.BridgeManagerInputPort;
import varga.weave.job.port.InfrastructureManagerInputPort;
import varga.weave.job.port.JobManagerInputPort;
import varga.weave.job.port.ProjectManagerInputPort;
import varga.weave.core.ByteCapacity;
import varga.weave.core.Tenant;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.util.unit.DataSize;
import reactor.core.publisher.Mono;

import static org.mockito.Mockito.when;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        classes = {YarnBridgeConfig.class, ApiConfiguration.class})
public class YarnHandlerTests {

    @Autowired
    private WebTestClient webTestClient;

    @MockBean
    private TenantWebArgumentResolver tenantWebArgumentResolver;

    @MockBean
    private ProjectManagerInputPort projectManagerInputPort;

    @MockBean
    private JobManagerInputPort jobManagerInputPort;

    @MockBean
    private InfrastructureManagerInputPort infrastructureManagerInputPort;

    @MockBean
    private BridgeManagerInputPort bridgeManagerInputPort;

    private Tenant tenant;

    @BeforeEach
    public void before() {
        Mockito.reset(this.tenantWebArgumentResolver, this.jobManagerInputPort, this.bridgeManagerInputPort);
        this.tenant = new Tenant();
    }

    @AfterEach
    public void after() {
        Mockito.verifyNoMoreInteractions(this.tenantWebArgumentResolver, this.jobManagerInputPort, this.bridgeManagerInputPort);
    }

    @Test
    @DisplayName("New application")
    public void newApplication() {

        // Given
        Bridge bridge = new Bridge();
        bridge.setId("bridge1");
        bridge.setType("yarn");

        TargetInfo targetInfo = new TargetInfo();
        targetInfo.setTotalCpus(new Capacity(0, 100, 50));
        targetInfo.setTotalMemory(new ByteCapacity(DataSize.ofGigabytes(0), DataSize.ofGigabytes(500), DataSize.ofGigabytes(250)));

        when(this.tenantWebArgumentResolver.getTenant(Mockito.any(), Mockito.booleanThat(r -> r), Mockito.booleanThat(r -> !r))).thenReturn(Mono.just(this.tenant));
        when(this.bridgeManagerInputPort.findBridgeById(this.tenant, "bridge1")).thenReturn(Mono.just(bridge));
        when(this.infrastructureManagerInputPort.findCapacityByInfrastructureById(this.tenant, "TODO")).thenReturn(Mono.just(targetInfo));

        // When
        WebTestClient.ResponseSpec actions = this.webTestClient
                .post()
                .uri("/tenants/letenant/bridges/bridge1/ws/v1/cluster/apps/new-application")
                .accept(MediaType.APPLICATION_JSON)
                .exchange();

        // Then
        actions
                .expectStatus().isOk()
                .expectHeader().contentType(MediaType.APPLICATION_JSON)
                .expectBody().json("""
                        {
                            "application-id":"application_1409421698529_0012",
                            "maximum-resource-capability": {
                                "memory":"512000",
                                "vCores":"100"
                            }
                        }
                        """);
        Mockito.verify(this.bridgeManagerInputPort).findBridgeById(this.tenant, "bridge1");
        Mockito.verify(this.infrastructureManagerInputPort).findCapacityByInfrastructureById(this.tenant, "TODO");
        Mockito.verify(this.tenantWebArgumentResolver).getTenant(Mockito.any(), Mockito.anyBoolean(), Mockito.anyBoolean());
    }

    @Test
    @DisplayName("Create application")
    public void createApplication() {

        // Given
        Bridge bridge = new Bridge();
        bridge.setId("bridge1");
        bridge.setType("yarn");

        Project project = new Project();
        project.setId("project-id");

        Job job = new Job();
        job.setId("job-id");

        JobRun run = new JobRun();
        run.setId("run-id");

        when(this.tenantWebArgumentResolver.getTenant(Mockito.any(), Mockito.booleanThat(r -> r), Mockito.booleanThat(r -> !r))).thenReturn(Mono.just(this.tenant));
        when(this.bridgeManagerInputPort.findBridgeById(this.tenant, "bridge1")).thenReturn(Mono.just(bridge));
        when(this.projectManagerInputPort.findProjectById(this.tenant, "project-id")).thenReturn(Mono.just(project));
        when(this.jobManagerInputPort.createJob(Mockito.eq(tenant), Mockito.eq(project), Mockito.any())).thenReturn(Mono.just(job));
        when(this.jobManagerInputPort.createRunForJob(Mockito.eq(tenant), Mockito.eq(job.getId()), Mockito.any())).thenReturn(Mono.just(run));

        // When
        WebTestClient.ResponseSpec actions = this.webTestClient
                .post()
                .uri("/tenants/letenant/bridges/bridge1/ws/v1/cluster/apps")
                .accept(MediaType.APPLICATION_JSON)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("""
                        {
                          "am-container-spec": {
                            "commands": {
                              "command": "{{JAVA_HOME}}/bin/java -server -Xmx1024m -Dhdp.version=2.4.0.0-169 -Dspark.yarn.app.container.log.dir=/hadoop/yarn/log/rest-api -Dspark.app.name=SimpleProject org.apache.spark.deploy.yarn.ApplicationMaster --class IrisApp --jar __app__.jar --arg '--class' --arg 'SimpleProject' 1><LOG_DIR>/AppMaster.stdout 2><LOG_DIR>/AppMaster.stderr"
                            },
                            "environment": {
                              "entry": [
                                {
                                  "key": "WEAVE_PROJECT",
                                  "value": "project-id"
                                },
                                {
                                  "key": "JAVA_HOME",
                                  "value": "/usr/jdk64/jdk1.8.0_60/"
                                },
                                {
                                  "key": "SPARK_YARN_MODE",
                                  "value": true
                                },
                                {
                                  "key": "HDP_VERSION",
                                  "value": "2.4.0.0-169"
                                },
                                {
                                  "key": "CLASSPATH",
                                  "value": "{{PWD}}<CPS>__spark__.jar<CPS>{{PWD}}/__app__.jar<CPS>{{PWD}}/__app__.properties<CPS>{{HADOOP_CONF_DIR}}<CPS>/usr/hdp/current/hadoop-client/*<CPS>/usr/hdp/current/hadoop-client/lib/*<CPS>/usr/hdp/current/hadoop-hdfs-client/*<CPS>/usr/hdp/current/hadoop-hdfs-client/lib/*<CPS>/usr/hdp/current/hadoop-yarn-client/*<CPS>/usr/hdp/current/hadoop-yarn-client/lib/*<CPS>{{PWD}}/mr-framework/hadoop/share/hadoop/common/*<CPS>{{PWD}}/mr-framework/hadoop/share/hadoop/common/lib/*<CPS>{{PWD}}/mr-framework/hadoop/share/hadoop/yarn/*<CPS>{{PWD}}/mr-framework/hadoop/share/hadoop/yarn/lib/*<CPS>{{PWD}}/mr-framework/hadoop/share/hadoop/hdfs/*<CPS>{{PWD}}/mr-framework/hadoop/share/hadoop/hdfs/lib/*<CPS>{{PWD}}/mr-framework/hadoop/share/hadoop/tools/lib/*<CPS>/usr/hdp/2.4.0.0-169/hadoop/lib/hadoop-lzo-0.6.0.2.4.0.0-169.jar<CPS>/etc/hadoop/conf/secure<CPS>"
                                },
                                {
                                  "key": "SPARK_YARN_CACHE_FILES",
                                  "value": "hdfs://beebox01.localdomain:8020/tmp/simple-project/simple-project.jar#__app__.jar,hdfs://beebox01.localdomain:8020/hdp/apps/2.4.0.0-169/spark/spark-hdp-assembly.jar#__spark__.jar"
                                },
                                {
                                  "key": "SPARK_YARN_CACHE_FILES_FILE_SIZES",
                                  "value": "10588,191724610"
                                },
                                {
                                  "key": "SPARK_YARN_CACHE_FILES_TIME_STAMPS",
                                  "value": "1460990579987,1460219553714"
                                },
                                {
                                  "key": "SPARK_YARN_CACHE_FILES_VISIBILITIES",
                                  "value": "PUBLIC,PRIVATE"
                                }
                              ]
                            },
                            "local-resources": {
                              "entry": [
                                {
                                  "key": "__spark__.jar",
                                  "value": {
                                    "resource": "hdfs://beebox01.localdomain:8020/hdp/apps/2.4.0.0-169/spark/spark-hdp-assembly.jar",
                                    "size": 191724610,
                                    "timestamp": 1460219553714,
                                    "type": "FILE",
                                    "visibility": "APPLICATION"
                                  }
                                },
                                {
                                  "key": "__app__.jar",
                                  "value": {
                                    "resource": "hdfs://beebox01.localdomain:8020/tmp/simple-project/simple-project.jar",
                                    "size": 10588,
                                    "timestamp": 1460990579987,
                                    "type": "FILE",
                                    "visibility": "APPLICATION"
                                  }
                                },
                                {
                                  "key": "__app__.properties",
                                  "value": {
                                    "resource": "hdfs://beebox01.localdomain:8020/tmp/simple-project/spark-yarn.properties",
                                    "size": 762,
                                    "timestamp": 1460990580053,
                                    "type": "FILE",
                                    "visibility": "APPLICATION"
                                  }
                                }
                              ]
                            }
                          },
                          "application-id": "application_1460195242962_0055",
                          "application-name": "SimpleProject",
                          "application-type": "YARN",
                          "keep-containers-across-application-attempts": false,
                          "max-app-attempts": 2,
                          "resource": {
                            "memory": 1024,
                            "vCores": 1
                          },
                          "unmanaged-AM": false
                        }
                        """)
                .exchange();

        // Then
        actions
                .expectStatus().isAccepted()
                .expectHeader().contentType(MediaType.APPLICATION_JSON)
                .expectHeader().location("");

        Mockito.verify(this.bridgeManagerInputPort).findBridgeById(this.tenant, "bridge1");
        Mockito.verify(this.tenantWebArgumentResolver).getTenant(Mockito.any(), Mockito.anyBoolean(), Mockito.anyBoolean());
    }
}
