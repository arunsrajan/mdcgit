echo STARTING DataCruncher....
JMXPORT=33330
DEBUGPORT=4000
JMXCONFIG="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=$JMXPORT -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"
DEBUGCONFIG="-Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=${DEBUGPORT},suspend=n"
MEMCONFIG="-Xms2G -Xmx2G"
GCCONFIG="-XX:+UseG1GC"
IPV4="-Djava.net.preferIPv4Stack=true"
CLASSNAME=com.github.mdc.stream.submitter.StreamPipelineJobSubmitter
java --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.lang.invoke=ALL-UNNAMED -classpath ".:../lib/*:../modules/*" ${MEMCONFIG} ${GCCCONFIG} ${IPV4} ${CLASSNAME} $@
