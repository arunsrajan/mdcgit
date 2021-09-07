#!/usr/bin/bash
echo STARTING DataCruncher Task Scheduler....
export JMXPORT=33330
export DEBUGPORT=4000
export PORTOFFexport=0
export JMXCONFIG="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=$JMXPORT -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"
export ZOOKEEPERADMINCONFIG="-Dzookeeper.admin.serverPort=$ZOOADMINPORT"
export CLASSPATH="-cp '.:../lib/*:../modules/*'"
export DEBUGCONFIG="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=$DEBUGPORT"
export CLASSNAME=com.github.mdc.stream.scheduler.MassiveDataStreamTaskSchedulerDaemon
mesos-master &
cd /opt/mdc/bin
echo java -classpath ".:/opt/mdc/lib/*:/opt/mdc/modules/*" "-Dzookeeper.hostport=$ZKHOSTPORT" "-Dtaskschedulerstream.host=$HOST" "-Dtaskschedulerstream.port=$PORT" $MEMCONFIGLOW $MEMCONFIGHIGH $GCCCONFIG -Djava.net.preferIPv4Stack=true $CLASSNAME
/usr/local/java/jdk-15.0.1/bin/java -classpath ".:/opt/mdc/lib/*:/opt/mdc/modules/*" "-Djava.library.path=/usr/local/lib" "-Dtaskschedulerstream.isjgroups=$ISJGROUPS" "-Dtaskschedulerstream.isyarn=$ISYARN" "-Dtaskschedulerstream.ismesos=$ISMESOS" "-Dtaskschedulerstream.mesosmaster=$MESOSMASTER" "-Dzookeeper.hostport=$ZKHOSTPORT" "-Dtaskschedulerstream.host=$HOST" "-Dtaskschedulerstream.port=$PORT" $DEBUGCONFIG $MEMCONFIGLOW $MEMCONFIGHIGH $GCCCONFIG -Djava.net.preferIPv4Stack=true $CLASSNAME

