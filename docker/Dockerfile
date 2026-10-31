# One Dockerfile, three build targets. The "base" stage holds Java and the HBase
# distribution; "hbase" runs it as a server, "flume" reuses it as a client
# library so Flume's HBase sink has the jars it needs.
#
#   docker build --target hbase -f docker/Dockerfile .
#   docker build --target flume -f docker/Dockerfile .

ARG JAVA_IMAGE=eclipse-temurin:11-jre

# ---------------------------------------------------------------- base
FROM ${JAVA_IMAGE} AS base

ARG HBASE_VERSION=2.5.16
ARG APACHE_DIST=https://archive.apache.org/dist

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates procps \
 && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL "${APACHE_DIST}/hbase/${HBASE_VERSION}/hbase-${HBASE_VERSION}-bin.tar.gz" \
    | tar -xz -C /opt \
 && ln -s "/opt/hbase-${HBASE_VERSION}" /opt/hbase

ENV HBASE_HOME=/opt/hbase
ENV PATH="${HBASE_HOME}/bin:${PATH}"

# ---------------------------------------------------------------- hbase
FROM base AS hbase

COPY hbase/init-table.sh /usr/local/bin/init-table.sh
RUN chmod +x /usr/local/bin/init-table.sh

EXPOSE 2181 16000 16010 16020 16030

# "hbase master start" in standalone mode runs the Master, a RegionServer and
# an embedded ZooKeeper in this single foreground process.
CMD ["hbase", "master", "start"]

# ---------------------------------------------------------------- flume
FROM base AS flume

ARG FLUME_VERSION=1.11.0
ARG APACHE_DIST=https://archive.apache.org/dist

RUN curl -fsSL "${APACHE_DIST}/flume/${FLUME_VERSION}/apache-flume-${FLUME_VERSION}-bin.tar.gz" \
    | tar -xz -C /opt \
 && ln -s "/opt/apache-flume-${FLUME_VERSION}-bin" /opt/flume

ENV FLUME_HOME=/opt/flume
ENV PATH="${FLUME_HOME}/bin:${PATH}"

RUN mkdir -p /var/flume

# flume-ng hard-codes -Xmx20m, which is far too small for the HBase client and a
# file-channel backlog (the agent dies with OutOfMemoryError). FLUME_JAVA_OPTS
# is appended after that default, so the larger -Xmx here wins.
ENV FLUME_JAVA_OPTS="-Xms256m -Xmx768m"

# flume-ng appends `hbase classpath` (found through HBASE_HOME) to its own, so
# the HBase client jars and ${HBASE_HOME}/conf/hbase-site.xml are visible to the
# HBase sink. The conf file is mounted at runtime, see docker-compose.yml.

EXPOSE 34545

CMD ["flume-ng", "agent", \
     "--conf", "/opt/flume/conf", \
     "--conf-file", "/opt/flume/conf/flume.conf", \
     "--name", "agent", \
     "-Dflume.root.logger=INFO,console", \
     "-Dflume.monitoring.type=http", \
     "-Dflume.monitoring.port=34545"]
