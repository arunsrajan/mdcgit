#!/usr/bin/bash
echo STARTING DataCruncher Task Scheduler....
export JMXPORT=33330
export DEBUGPORT=4000
export PORTOFFexport=0
export JMXCONFIG="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=$JMXPORT -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"
export ZOOKEEPERADMINCONFIG="-Dzookeeper.admin.serverPort=$ZOOADMINPORT"
export CLASSPATH="-cp '.:../lib/*:../modules/*'"
export DEBUGCONFIG="-Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=$DEBUGPORT,suspend=n -Dorg.singam.debug.port=$DEBUGPORT"
java -cp ".:/opt/mdc/lib/*:/opt/mdc/modules/*" "-Dzookeeper.hostport=$ZKHOSTPORT" "-Dtaskscheduler.host=$HOST" "-Dtaskscheduler.port=$PORT" $MEMCONFIGLOW $MEMCONFIGHIGH $GCCCONFIG -Djava.net.preferIPv4Stack=true com.github.mdc.tasks.scheduler.TaskSchedulerRunner