package varga.weave;

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

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import varga.weave.core.*;
import varga.weave.virtual.*;

@Configuration
public class VirtualBridgeConfig {

    @Bean
    @ConditionalOnMissingBean
    public TenantManagerInputPort tenantManagerInputPort() {
        return new VirtualTenantManagerAdapter();
    }

    @Bean
    @ConditionalOnMissingBean
    public InfrastructureManagerInputPort infrastructureManagerInputPort() {
        return new VirtualInfrastructureManagerAdapter();
    }

    @Bean
    @ConditionalOnMissingBean
    public BridgeManagerInputPort bridgeManagerInputPort() {
        return new VirtualBridgeManagerAdapter();
    }

    @Bean
    @ConditionalOnMissingBean
    public ProjectManagerInputPort projectManagerInputPort() {
        return new VirtualProjectManagerAdapter();
    }

    @Bean
    @ConditionalOnMissingBean
    public JobManagerInputPort jobManagerInputPort() {
        return new VirtualJobManagerAdapter();
    }

    @Bean
    @ConditionalOnMissingBean
    public DataManagerInputPort dataManagerInputPort() {
        return new VirtualDataManagerAdapter();
    }

    @Bean
    @ConditionalOnMissingBean
    public ApplicationRepository applicationRepository(VirtualApplicationRepository virtualApplicationRepository) {
        return virtualApplicationRepository;
    }

    @Bean
    @ConditionalOnMissingBean
    public ClusterRepository clusterRepository(VirtualClusterRepository virtualClusterRepository) {
        return virtualClusterRepository;
    }

    @Bean
    @ConditionalOnMissingBean
    public ContainerRepository containerRepository(VirtualContainerRepository virtualContainerRepository) {
        return virtualContainerRepository;
    }

    @Bean
    @ConditionalOnMissingBean
    public UserRepositoryAdapter userRepositoryAdapter(VirtualUserRepositoryAdapter virtualUserRepositoryAdapter) {
        return virtualUserRepositoryAdapter;
    }

    @Bean
    @ConditionalOnMissingBean
    public ApplicationRepositoryAdapter applicationRepositoryAdapter(VirtualApplicationRepositoryAdapter virtualApplicationRepositoryAdapter) {
        return virtualApplicationRepositoryAdapter;
    }
}
