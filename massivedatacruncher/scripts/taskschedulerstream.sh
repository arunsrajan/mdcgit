echo STARTING DataCruncher Task Scheduler....
export JMXPORT=33330
export DEBUGPORT=4000
export PORTOFFexport=0
export JMXCONFIG="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=$JMXPORT -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"
export ZOOKEEPERADMINCONFIG="-Dzookeeper.admin.serverPort=$ZOOADMINPORT"
export CLASSPATH="-cp '.:../lib/*:../modules/*'"
export DEBUGCONFIG="-Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=$DEBUGPORT,suspend=n -Dorg.singam.debug.port=$DEBUGPORT"
export CLASSNAME=com.github.mdc.stream.scheduler.StreamPipelineTaskSchedulerRunner
echo java -classpath ".:/opt/mdc/lib/*:/opt/mdc/modules/*" "-Dzookeeper.hostport=$ZKHOSTPORT" "-Dtaskschedulerstream.host=$HOST" "-Dtaskschedulerstream.port=$PORT" $MEMCONFIGLOW $MEMCONFIGHIGH $GCCCONFIG -Djava.net.preferIPv4Stack=true $CLASSNAME
/usr/local/java/jdk-17.0.2/bin/java --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.lang.invoke=ALL-UNNAMED -classpath ".:../lib/*:../modules/*" $MEMCONFIGLOW $MEMCONFIGHIGH $GCCCONFIG -Djava.net.preferIPv4Stack=true $CLASSNAME

