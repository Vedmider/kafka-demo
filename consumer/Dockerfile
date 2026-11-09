# syntax=docker/dockerfile:1.7
# Multi-module build: compiles parent + common + consumer, runs only the consumer jar.
#   docker build -f consumer/Dockerfile --build-arg REGISTRY=<mirror>/library -t kafka-demo-consumer:0.1.0 .
# Optional private-mirror settings as a BuildKit secret:
#   --secret id=m2settings,src=$HOME/.m2/settings.xml
ARG REGISTRY=docker.io/library
ARG BUILD_IMAGE=${REGISTRY}/maven:3.9-eclipse-temurin-21
ARG RUNTIME_IMAGE=${REGISTRY}/eclipse-temurin:21-jre

FROM ${BUILD_IMAGE} AS build
WORKDIR /build
COPY pom.xml .
COPY common/pom.xml common/pom.xml
COPY producer/pom.xml producer/pom.xml
COPY consumer/pom.xml consumer/pom.xml
COPY common/src common/src
COPY consumer/src consumer/src
RUN --mount=type=secret,id=m2settings,target=/root/.m2/settings.xml,required=false \
    mvn -B $([ -s /root/.m2/settings.xml ] && echo -s /root/.m2/settings.xml) \
        -pl common,consumer -am -DskipTests clean package

FROM ${RUNTIME_IMAGE} AS runtime
WORKDIR /app
RUN groupadd -r app -g 10001 && useradd -r -g app -u 10001 -s /usr/sbin/nologin app
COPY --from=build /build/consumer/target/consumer-*.jar /app/app.jar
USER 10001
EXPOSE 8082
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
