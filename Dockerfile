FROM openjdk:8-alpine

# bash: for spark shell
# procps: for `ps -p`
# might need 'R-dev' for install CRAN package
RUN apk --no-cache add bash procps python R maven

# specify some env variables which are used later
ENV SPARK_VERSION 2.1.1
ENV HADOOP_VERSION 2.7
ENV TIDB_VERSION latest
ENV SPARK_HOME /usr/local/share/spark
ENV TIDB_HOME /usr/local/share/tidb
ENV DATA_HOME /usr/local/share/data
ENV LOG_HOME /usr/local/share/log
ENV SPARK_NO_DAEMONIZE=true
ENV PD_ADDR 127.0.0.1:2379
# download tidb, tikv, and pd with version
RUN set -xe \
  && cd tmp \
  && wget http://download.pingcap.org/tidb-${TIDB_VERSION}-linux-amd64-unportable.tar.gz \
  && tar -zxvf tidb-${TIDB_VERSION}-linux-amd64-unportable.tar.gz \
  && rm *.tar.gz \
  && mv tidb-${TIDB_VERSION}-linux-amd64-unportable ${TIDB_HOME}

RUN set -xe \
  && ${TIDB_HOME}/pd-server --data-dir=${DATA_HOME}/pd --log-file=${LOG_HOME}/pd.log >/dev/null 2>&1


RUN set -xe \
  && sleep 10s
  && ${TIDB_HOME}/tikv-server --pd=$PD_ADDR --store=tikv -C ./etc/config-template.toml --log-file ${LOG_HOME}/tikv.log 2>&1

RUN set -xe \
  && ${TIDB_HOME}/tidb-server --store=tikv --path=$PD_ADDR --log-file=${LOG_HOME}/tidb.log 2>&1

# download spark with version
RUN set -xe \
  && cd tmp \
  && wget https://archive.apache.org/dist/spark/spark-2.1.1/spark-2.1.1-bin-hadoop2.7.tgz \
  && tar -zxvf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
  && rm *.tgz \
  && mkdir -p `dirname ${SPARK_HOME}` \
  && mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME}

ENV PATH=$PATH:${SPARK_HOME}/sbin:${SPARK_HOME}/bin

# add current dir as workdir
ADD  . /tispark
WORKDIR /tispark
# assume user already build tispark project
RUN cp /tispark/core/target/tispark-core-0.1.0-SNAPSHOT-jar-with-dependencies.jar ${SPARK_HOME}/jars
# load data first

# run intergation test 
# RUN bash test-all.sh
RUN rm -rf /tispark
