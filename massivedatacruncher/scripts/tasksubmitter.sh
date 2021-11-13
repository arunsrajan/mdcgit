echo STARTING DataCruncher....
export JMXPORT=33330
export DEBUGPORT=4000
export PORTOFFexport=0
export JMXCONFIG="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=$JMXPORT -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"
export ZOOKEEPERADMINCONFIG="-Dzookeeper.admin.serverPort=$ZOOADMINPORT"
export CLASSPATH="-cp '.:../lib/*:../modules/*'"
export DEBUGCONFIG="-Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=$DEBUGPORT,suspend=n -Dorg.singam.debug.port=$DEBUGPORT"
export MEMCONFIG="-Xms256m -Xmx256m"
export GCCONFIG="-XX:+UseG1GC -XX:+CMSClassUnloadingEnabled -Dsun.rmi.dgc.client.gcInterval=3600000"
/usr/local/java/jdk-15.0.2/bin/java --enable-preview -classpath ".:../lib/*:../modules/*" -Xms256M -Xmx256M $GCCCONFIG -Djava.net.preferIPv4Stack=true com.github.mdc.tasks.scheduler.ApplicationSubmitter "$@"