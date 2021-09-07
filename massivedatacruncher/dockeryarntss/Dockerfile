FROM amd64/openjdk:16-jdk-oracle
ADD "openjdk-15.0.1_linux-x64_bin.tar.gz" /usr/local/java
RUN echo export JAVA_HOME=/usr/local/java/jdk-15.0.1 >> ~/.bashrc
RUN echo export PATH=/usr/local/java/jdk-15.0.1/bin:$PATH >> ~/.bashrc
ADD "MassiveDataCruncher-1.0.0-64bit.tar.gz" /opt/mdc
RUN mkdir -p /opt/mdc/appjar
RUN chmod 777 /opt/mdc/appjar
COPY mdctss.properties /opt/mdc/config/mdc.properties
COPY taskschedulerstream.sh /opt/mdc/bin/taskschedulerstream.sh
RUN cd /opt/mdc/bin
WORKDIR /opt/mdc/bin
ENTRYPOINT ["sh","taskschedulerstream.sh"]