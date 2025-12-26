# syntax=docker/dockerfile:1

# --- Build stage: build native binary with Maven (plugin downloads GraalVM toolchain) ---
FROM maven:3.9.9-eclipse-temurin-17 AS build

WORKDIR /workspace
COPY pom.xml .
COPY src ./src

# Build native binary
RUN --mount=type=cache,target=/root/.m2 \
    mvn -Pnative -DskipTests -Dstyle.color=always -B \
        clean package org.graalvm.buildtools:native-maven-plugin:compile-no-fork

# The native binary name is usually the artifactId
RUN ls -lah target && \
    (test -f target/weave-yarn-bridge || test -f target/application) || (echo "Native binary not found in target/. Check native-maven-plugin config." && exit 1)

# --- Runtime stage: minimal image for native binary ---
FROM gcr.io/distroless/cc-debian12:nonroot

WORKDIR /app

# Copy native binary (choose existing name)
COPY --from=build /workspace/target/weave-yarn-bridge /app/weave-yarn-bridge
USER nonroot

EXPOSE 8080

ENTRYPOINT ["/app/weave-yarn-bridge"]
