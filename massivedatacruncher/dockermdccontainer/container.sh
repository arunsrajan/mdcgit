echo STARTING DataCruncher Task Executor....
export JMXPORT=33330
export DEBUGPORT=4000
export ZOOKEEPERADMINCONFIG="-Dzookeeper.admin.serverPort=$ZOOADMINPORT"
export CLASSPATH="-cp '.:../lib/*:../modules/*'"
export DEBUGCONFIG="-Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=$DEBUGPORT,suspend=n"
/usr/local/java/jdk-17.0.2/bin/java --enable-preview --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.lang.invoke=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util.concurrent.atomic=ALL-UNNAMED "-Dzookeeper.hostport=$ZKHOSTPORT" "-Dtaskexecutor.host=$HOST" "-Dtaskexecutor.port=$PORT" "-Dnode.port=$NODEPORT" -classpath ".:../lib/*:../modules/*" $MEMCONFIGLOW $MEMCONFIGHIGH $GCCCONFIG -Djava.net.preferIPv4Stack=true com.github.mdc.tasks.executor.NodeLauncher