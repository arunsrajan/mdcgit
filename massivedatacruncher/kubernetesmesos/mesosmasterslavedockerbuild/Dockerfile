FROM mesos/mesos-mini:1.9.x-2020-06-30
USER root

ADD "MassiveDataCruncher-1.0.0-64bit.tar.gz" /opt/mdc
RUN mkdir -p /opt/mdc/appjar
RUN chmod 777 /opt/mdc/appjar
COPY mdctss.properties /opt/mdc/config/mdc.properties
RUN cd /opt/mdc/bin
WORKDIR /opt/mdc/bin

ADD "openjdk-15.0.1_linux-x64_bin.tar.gz" /usr/local/java

RUN echo export JAVA_HOME=/usr/local/java/jdk-15.0.1 >> ~/.bashrc

RUN echo export PATH=/usr/local/java/jdk-15.0.1/bin:$PATH >> ~/.bashrc

ENV MESOS_SYSTEMD_ENABLE_SUPPORT false

ENTRYPOINT ["mesos-slave"]