FROM flink:1.20-java17

USER root

RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop \
    && cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/s3-fs-hadoop/

COPY target/realrisk-flink-job-0.1.0-SNAPSHOT.jar /opt/flink/usrlib/realrisk-flink-job-0.1.0-SNAPSHOT.jar

RUN chown flink:flink /opt/flink/usrlib/realrisk-flink-job-0.1.0-SNAPSHOT.jar

USER flink
