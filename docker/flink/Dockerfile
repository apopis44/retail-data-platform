FROM maven:3.9.11-eclipse-temurin-17 AS connector-downloader

WORKDIR /build

COPY docker/flink/pom.xml ./pom.xml

RUN mvn --batch-mode --no-transfer-progress \
        org.apache.maven.plugins:maven-dependency-plugin:3.8.1:copy \
        -Dartifact=org.apache.flink:flink-sql-connector-kafka:3.4.0-1.20:jar \
        -DoutputDirectory=/connectors \
    && mvn --batch-mode --no-transfer-progress \
        org.apache.maven.plugins:maven-dependency-plugin:3.8.1:copy-dependencies \
        -DincludeScope=runtime \
        -DoutputDirectory=/connectors \
        -DexcludeGroupIds=org.apache.flink,org.slf4j,junit,org.junit.vintage,org.junit.platform,org.opentest4j,org.apiguardian,org.hamcrest

FROM flink:1.20.5-scala_2.12-java17

USER root

RUN mkdir -p /opt/flink/connectors /opt/flink/checkpoints \
    && chown -R flink:flink /opt/flink/connectors /opt/flink/checkpoints

USER flink

COPY --from=connector-downloader \
    --chown=flink:flink \
    /connectors/ \
    /opt/flink/connectors/