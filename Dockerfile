FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY . .
RUN mvn -B verify
FROM eclipse-temurin:17-jre AS ingestion
WORKDIR /app
RUN mkdir data && chown -R 10001:10001 /app
COPY --from=build /build/ingestion/target/telemetry-ingestion-1.0.0.jar app.jar
USER 10001
EXPOSE 8080
ENTRYPOINT ["java","-jar","app.jar"]
FROM flink:1.20.5-scala_2.12-java17 AS flink
USER root
RUN mkdir -p /opt/flink/checkpoints && chown -R flink:flink /opt/flink/checkpoints
COPY --from=build /build/stream-job/target/telemetry-stream-job-1.0.0.jar /opt/flink/usrlib/telemetry-job.jar
USER flink
