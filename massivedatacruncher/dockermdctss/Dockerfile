FROM openjdk:15.0.2-jdk-oraclelinux7
ADD "MassiveDataCruncher-1.0-jdk15.tar.gz" /opt/mdc
RUN mkdir -p /opt/mdc/appjar
RUN chmod 777 /opt/mdc/appjar
COPY mdctss.properties /opt/mdc/config/mdc.properties
COPY taskschedulerstream.sh /opt/mdc/bin/taskschedulerstream.sh
RUN cd /opt/mdc/bin
WORKDIR /opt/mdc/bin
ENTRYPOINT ["sh","taskschedulerstream.sh"]