FROM bde2020/hadoop-datanode
ADD "MassiveDataCruncher-1.0-jdk15.tar.gz" /opt/mdc
COPY mdcstandalone.properties /opt/mdc/config/mdc.properties
COPY standalone.sh /opt/mdc/bin/standalone.sh

RUN cd /
WORKDIR /

COPY entry.sh /entry.sh

ADD "openjdk-15.0.2_linux-x64_bin.tar.gz" /usr/local/java

RUN cd /
WORKDIR /
ENV CORE_CONF_fs_defaultFS=hdfs://namenode:9000
ENV HDFS_CONF_dfs_datanode_data_dir=file:///hadoop/dfs/data
RUN mkdir -p /hadoop/dfs/data
VOLUME /hadoop/dfs/data

RUN chmod 777 /hadoop/dfs/data

EXPOSE 9864
ENTRYPOINT ["./entry.sh"]