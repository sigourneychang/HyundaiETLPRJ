# ARM64 native Hadoop 2.7.4 + Hive 2.3.2 (one image for namenode / datanode / metastore / hiveserver2)
#
# Why: the bde2020 images are amd64-only. Under Rosetta, HiveServer2's JVM stalled for 6-10 minutes
# at random (a finished local MapReduce job was noticed 10 minutes late), and every beeline call
# paid emulation start-up cost. This image runs the same Hadoop/Hive on a native arm64 JVM.
#
# Hadoop and Hive are Java, so the install trees are copied as-is from the bde2020 image (same
# versions, same extra jars such as the PostgreSQL JDBC driver); only the x86 native libraries are
# dropped (Hadoop falls back to its Java implementations). The start-up scripts and the
# env -> *-site.xml configuration (CORE_CONF_*, HDFS_CONF_*, HIVE_SITE_CONF_*) are the bde2020 ones,
# so docker-compose environment, paths (/opt/hadoop-2.7.4, /etc/hadoop, /opt/hive) and existing
# data volumes stay compatible.
#
#   build:  docker build -t hive2-arm:2.3.2 docker/arm

# COPY-only stage: nothing runs here, so no emulation is needed to read the amd64 image
FROM --platform=linux/amd64 mirror.gcr.io/bde2020/hive:2.3.2-postgresql-metastore AS bde2020

FROM mirror.gcr.io/library/eclipse-temurin:8-jdk-jammy

RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      perl netcat-openbsd curl procps bash \
 && rm -rf /var/lib/apt/lists/*

COPY --from=bde2020 /opt/hadoop-2.7.4 /opt/hadoop-2.7.4
COPY --from=bde2020 /opt/hive /opt/hive
# snappy-java 1.0.x bundled with Hadoop 2.7 / Hive 2.3 has no aarch64 native code (Snappy-compressed
# Parquet/ORC fail with FAILED_TO_LOAD_NATIVE_LIBRARY). 1.1.8.4 is API compatible and ships aarch64.
RUN curl -fsSL -o /tmp/snappy-java-1.1.8.4.jar \
      https://repo1.maven.org/maven2/org/xerial/snappy/snappy-java/1.1.8.4/snappy-java-1.1.8.4.jar \
 && for j in $(find /opt -name 'snappy-java-1.0*.jar'); do \
      cp /tmp/snappy-java-1.1.8.4.jar "$(dirname "$j")/" && rm -f "$j"; done \
 && rm /tmp/snappy-java-1.1.8.4.jar
RUN rm -rf /opt/hadoop-2.7.4/lib/native/* \
 && rm -rf /etc/hadoop && mkdir -p /etc/hadoop /hadoop/dfs/name /hadoop/dfs/data /opt/hive/conf

# pristine configs from the bde2020 image (empty *-site.xml; filled from env vars at start-up)
COPY from-bde2020/hadoop-conf/ /etc/hadoop/
COPY from-bde2020/hive/conf/ /opt/hive/conf/
COPY from-bde2020/hive/entrypoint.sh /usr/local/bin/entrypoint.sh
COPY from-bde2020/hive/startup.sh /usr/local/bin/startup.sh
COPY from-bde2020/namenode/run.sh /run-namenode.sh
COPY from-bde2020/datanode/run.sh /run-datanode.sh
COPY run-metastore.sh /run-metastore.sh
RUN chmod +x /usr/local/bin/entrypoint.sh /usr/local/bin/startup.sh /run-namenode.sh /run-datanode.sh /run-metastore.sh

ENV HADOOP_VERSION=2.7.4 \
    HADOOP_PREFIX=/opt/hadoop-2.7.4 \
    HADOOP_HOME=/opt/hadoop-2.7.4 \
    HADOOP_CONF_DIR=/etc/hadoop \
    HIVE_VERSION=2.3.2 \
    HIVE_HOME=/opt/hive \
    MULTIHOMED_NETWORK=1 \
    USER=root \
    HDFS_CONF_dfs_namenode_name_dir=file:///hadoop/dfs/name \
    HDFS_CONF_dfs_datanode_data_dir=file:///hadoop/dfs/data \
    PATH=/opt/hive/bin:/opt/hadoop-2.7.4/bin:/opt/java/openjdk/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

WORKDIR /opt
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["startup.sh"]
