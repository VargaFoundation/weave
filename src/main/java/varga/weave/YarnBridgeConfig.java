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

import com.fasterxml.jackson.databind.ObjectMapper;
import varga.weave.rest.YarnHandler;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerCodecConfigurer;
import org.springframework.http.codec.json.Jackson2JsonDecoder;
import org.springframework.http.codec.json.Jackson2JsonEncoder;
import org.springframework.web.reactive.config.EnableWebFlux;
import org.springframework.web.reactive.config.WebFluxConfigurer;
import org.springframework.web.reactive.function.server.RequestPredicates;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;

// https://github.com/yassineazzouz/cerastes/blob/90ceef51e399e319e6a94cb5e950f47dacf8821a/cerastes/channel.py#L161
@EnableWebFlux
@Configuration
@ComponentScan("varga.weave.yarn")
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class YarnBridgeConfig implements WebFluxConfigurer {

    private final ObjectMapper objectMapper;

    @Override
    public void configureHttpMessageCodecs(ServerCodecConfigurer configurer) {
        configurer.defaultCodecs().jackson2JsonEncoder(new Jackson2JsonEncoder(this.objectMapper));
        configurer.defaultCodecs().jackson2JsonDecoder(new Jackson2JsonDecoder(this.objectMapper));
    }

    @Bean
    public RouterFunction<?> yarnBridgeRouterFunction(
            YarnHandler yarnHandler
    ) {
        return RouterFunctions.route()
                .path("/tenants/{tenant_id}/bridges/{bridge_id}/ws/v1/cluster/apps", apps -> apps
                        .path("", action -> action
                                .POST("", RequestPredicates.accept(MediaType.APPLICATION_JSON).and(RequestPredicates.contentType(MediaType.APPLICATION_JSON)),
                                        yarnHandler::createApplication)
                        )
                        .path("new-application", action -> action
                                .POST("", yarnHandler::newApplication)
                        )
                        .path("{application_id}", action -> action
                                .GET("", RequestPredicates.accept(MediaType.APPLICATION_JSON).and(RequestPredicates.contentType(MediaType.APPLICATION_JSON)),
                                        yarnHandler::getApplication)
                                .PUT("", RequestPredicates.accept(MediaType.APPLICATION_JSON).and(RequestPredicates.contentType(MediaType.APPLICATION_JSON)),
                                        yarnHandler::updateApplication)
                        )
                        .path("{application_id}/state", action -> action
                                .GET("", RequestPredicates.accept(MediaType.APPLICATION_JSON).and(RequestPredicates.contentType(MediaType.APPLICATION_JSON)),
                                        yarnHandler::getApplicationState)
                        )
                )
                .build();
    }
}
