echo STARTING Standalone Schedulers....
export DEBUGPORT=4000
export ZOOADMINPORT=8040
export ZOOKEEPERADMINCONFIG="-Dzookeeper.admin.serverPort=$ZOOADMINPORT"
export CLASSPATH="-classpath .:../lib/*:../modules/*"
export DEBUGCONFIG="-Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=$DEBUGPORT,suspend=n"
export CLASSNAME=com.github.mdc.tasks.scheduler.executor.standalone.Standalone
echo java --add-opens=java.base/java.nio=ALL-UNNAMED --enable-preview -XX:ActiveProcessorCount=4 --add-modules jdk.compiler -Xms512m -Xmx3g $ZOOKEEPERADMINCONFIG $DEBUGCONFIG $CLASSPATH -Djava.net.preferIPv4Stack=true $CLASSNAME
java --add-opens=java.base/java.nio=ALL-UNNAMED --enable-preview -XX:ActiveProcessorCount=4 --add-modules jdk.compiler -Xms512m -Xmx3g $ZOOKEEPERADMINCONFIG $DEBUGCONFIG $CLASSPATH -Djava.net.preferIPv4Stack=true $CLASSNAME