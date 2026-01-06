### Weave Yarn Bridge

An autonomous Hadoop HRPC/YARN adapter exposing simple hooks to integrate with your own backend (e.g., create Spark jobs on Kubernetes). It reverse‑engineers HRPC (YARN protocols) and also provides a REST facade compatible with common YARN REST flows.

#### What you get
- HRPC server skeleton and YARN protocol services (client/master/container) to interact with legacy Hadoop/YARN clients.
- REST endpoints compatible with YARN’s application lifecycle for Spark-on-YARN style submissions.
- A pluggable hook interface so your code decides what to do when a Spark application creation request arrives (e.g., create a Kubernetes Job, submit to Spark Operator, trigger an airflow DAG, etc.).

#### Quick start
1. Add the dependency (build locally for now):
```
mvn -DskipTests install
```
Then in your app’s pom.xml:
```
<dependency>
  <groupId>varga.weave</groupId>
  <artifactId>weave-yarn-bridge</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

2. Provide a Spring bean implementing the hook interface:
```
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.server.ServerRequest;
import reactor.core.publisher.Mono;
import varga.weave.api.ApplicationCreateResult;
import varga.weave.api.SparkApplicationLifecycleHandler;
import rest.varga.weave.NewApplication;
import rest.varga.weave.YarnApplication;

import java.util.Map;

@Component
class MySparkHook implements SparkApplicationLifecycleHandler {
  public Mono<ApplicationCreateResult> onSparkAppCreate(YarnApplication req, Map<String,String> env, ServerRequest r) {
    // Example: create a Kubernetes Job here, then return an appId
    String appId = "application_" + System.currentTimeMillis() + "_0001";
    return Mono.just(new ApplicationCreateResult(appId, "ACCEPTED", Map.of()));
  }
  public Mono<NewApplication> onNewApplication(ServerRequest r) {
    NewApplication n = new NewApplication();
    n.setApplicationId("application_" + System.currentTimeMillis() + "_0001");
    NewApplication.MaximumResourceCapability cap = new NewApplication.MaximumResourceCapability();
    cap.setMemory("16384"); cap.setVCores("8"); n.setCapability(cap); return Mono.just(n);
  }
  public Mono<String> getApplicationState(String applicationId) { return Mono.just("ACCEPTED"); }
  public Mono<Void> updateApplication(String applicationId, String desiredState) { return Mono.empty(); }
}
```

3. REST endpoints (auto-configured by Spring):
- POST `/tenants/{tenant_id}/bridges/{bridge_id}/ws/v1/cluster/apps` — create Spark application (payload compatible with YARN Spark JSON). The hook `onSparkAppCreate` is invoked. Returns 202 Accepted and a `Location` header ending with the `applicationId`.
- POST `/tenants/{tenant_id}/bridges/{bridge_id}/ws/v1/cluster/apps/new-application` — obtain a new application (capabilities). Delegates to `onNewApplication`.
- GET `/tenants/{tenant_id}/bridges/{bridge_id}/ws/v1/cluster/apps/{application_id}/state` — returns `{ "state": "..." }` from `getApplicationState`.
- PUT `/tenants/{tenant_id}/bridges/{bridge_id}/ws/v1/cluster/apps/{application_id}` — expects `{ "state": "KILLED"|... }`, delegated to `updateApplication`.

4. HRPC/YARN protocols
The repository contains the HRPC/YARN protocol handling (Netty + Protobuf messages). The goal is to enable legacy Hadoop/YARN clients to talk to this bridge. Services are decoupled from concrete implementations and rely on a set of interfaces (InputPorts and Repositories).

To provide your own implementation for the backend or storage, implement the corresponding interface and provide it as a Spring `@Bean`. The default "virtual" implementations are annotated with `@ConditionalOnMissingBean` and will be replaced by yours.

Example of custom repository:
```java
@Component
public class MyCustomContainerRepository implements ContainerRepository {
    // Implement methods to store container information in a real DB
}
```

Interfaces you can override:
- `TenantManagerInputPort`, `InfrastructureManagerInputPort`, `BridgeManagerInputPort`, `ProjectManagerInputPort`, `JobManagerInputPort`, `DataManagerInputPort` (Core logic)
- `ApplicationRepository`, `ClusterRepository`, `ContainerRepository` (RPC state storage)
- `UserRepositoryAdapter`, `ApplicationRepositoryAdapter` (Authentication data)

#### Configuration
Auto‑configuration is provided via Spring factories and a router defined in `varga.weave.YarnBridgeConfig`. No extra configuration is required to expose the REST routes. To override behavior, just provide your own `SparkApplicationLifecycleHandler` bean (it will replace the default no‑op one).

#### License

##### Update third parties license file

Update the content of the file NOTICE:

mvn org.codehaus.mojo:license-maven-plugin:aggregate-add-third-party@aggregate-add-third-party

##### Update license header on files

Update license header on files

mvn org.codehaus.mojo:license-maven-plugin:update-file-header@update-file-header

#### Notes
- This library targets Java 17 and Spring Boot 3.3.x.
- Protocol buffers are generated with the included Maven plugin.
- Logging API is SLF4J; bring your own implementation.

#### Roadmap
- Finalize full decoupling of HRPC services from any legacy internal APIs.
- Provide examples for integrating with Spark on Kubernetes (Spark Operator and native Job).
- Add integration tests for HRPC flows.
