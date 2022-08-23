#!/bin/bash
echo STARTING DataCruncher Task Scheduler....
export JMXPORT=33330
export PORTOFFexport=0
export JMXCONFIG="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=$JMXPORT -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"
export ZOOKEEPERADMINCONFIG="-Dzookeeper.admin.serverPort=$ZOOADMINPORT"
export CLASSPATH="-classpath .:../lib/*:../modules/*"
export DEBUGCONFIG="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=$DEBUGPORT"
export CLASSNAME=com.github.mdc.stream.scheduler.StreamPipelineTaskSchedulerRunner
cd /opt/mdc/bin
echo /usr/local/java/jdk-17.0.2/bin/java --enable-preview --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.lang.invoke=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util.concurrent.atomic=ALL-UNNAMED $CLASSPATH "-Dtaskschedulerstream.isjgroups=$ISJGROUPS" "-Dtaskschedulerstream.isyarn=$ISYARN" "-Dtaskschedulerstream.ismesos=$ISMESOS" "-Dtaskschedulerstream.mesosmaster=$MESOSMASTER" "-Dzookeeper.hostport=$ZKHOSTPORT" "-Dtaskschedulerstream.host=$HOST" "-Dtaskschedulerstream.port=$PORT" $DEBUGCONFIG $MEMCONFIGLOW $MEMCONFIGHIGH $GCCCONFIG -Djava.net.preferIPv4Stack=true $CLASSNAME
/usr/local/java/jdk-17.0.2/bin/java --enable-preview --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.lang.invoke=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util.concurrent.atomic=ALL-UNNAMED $CLASSPATH "-Dtaskschedulerstream.isjgroups=$ISJGROUPS" "-Dtaskschedulerstream.isyarn=$ISYARN" "-Dtaskschedulerstream.ismesos=$ISMESOS" "-Dtaskschedulerstream.mesosmaster=$MESOSMASTER" "-Dzookeeper.hostport=$ZKHOSTPORT" "-Dtaskschedulerstream.host=$HOST" "-Dtaskschedulerstream.port=$PORT" $DEBUGCONFIG $MEMCONFIGLOW $MEMCONFIGHIGH $GCCCONFIG -Djava.net.preferIPv4Stack=true $CLASSNAME

