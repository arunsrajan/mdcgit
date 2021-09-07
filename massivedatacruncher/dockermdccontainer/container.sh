echo STARTING DataCruncher Task Executor....
export JMXPORT=33330
export DEBUGPORT=4000
export ZOOKEEPERADMINCONFIG="-Dzookeeper.admin.serverPort=$ZOOADMINPORT"
export CLASSPATH="-cp '.:../lib/*:../modules/*'"
export DEBUGCONFIG="-Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=$DEBUGPORT,suspend=n"
/usr/local/java/jdk-15.0.2/bin/java --enable-preview "-Dzookeeper.hostport=$ZKHOSTPORT" "-Dtaskexecutor.host=$HOST" "-Dtaskexecutor.port=$PORT" "-Dnode.port=$NODEPORT" -classpath ".:../lib/*:../modules/*" $MEMCONFIGLOW $MEMCONFIGHIGH $GCCCONFIG -Djava.net.preferIPv4Stack=true com.github.mdc.tasks.executor.ContainerLauncher