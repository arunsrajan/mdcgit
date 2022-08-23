echo STARTING Standalone Schedulers....
export DEBUGPORT=4000
export ZOOADMINPORT=8040
export ZOOKEEPERADMINCONFIG="-Dzookeeper.admin.serverPort=$ZOOADMINPORT"
export CLASSPATH="-classpath .:../lib/*:../modules/*"
export DEBUGCONFIG="-Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=$DEBUGPORT,suspend=n"
export CLASSNAME=com.github.mdc.tasks.scheduler.executor.standalone.EmbeddedSchedulersNodeLauncher
echo java --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.lang.invoke=ALL-UNNAMED -cp ".:../lib/*:../modules/*" $MEMCONFIGLOW $MEMCONFIGHIGH $GCCCONFIG -Djava.net.preferIPv4Stack=true $CLASSNAME
java --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.lang.invoke=ALL-UNNAMED -cp ".:../lib/*:../modules/*" $MEMCONFIGLOW $MEMCONFIGHIGH $GCCCONFIG -Djava.net.preferIPv4Stack=true $CLASSNAME