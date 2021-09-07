FROM openjdk:15.0.2-jdk-oraclelinux7
ADD "MassiveDataCruncher-1.0-jdk15.tar.gz" /opt/mdc
COPY mdcts.properties /opt/mdc/config/mdc.properties
COPY taskscheduler.sh /opt/mdc/bin/taskscheduler.sh
COPY tasksubmitter.sh /opt/mdc/bin/tasksubmitter.sh
RUN cd /opt/mdc/bin
WORKDIR /opt/mdc/bin
ENTRYPOINT ["sh","/opt/mdc/bin/taskscheduler.sh"]