title MDC Standlone

@echo off

setLocal EnableDelayedExpansion

echo STARTING DataCruncher....

SET DATACRUNCHER=%~dp0\\..

SET DATACRUNCHERLIB=%DATACRUNCHER%\lib

SET DATACRUNCHERMODULES=%DATACRUNCHER%\modules

set JMXPORT=33330

set DEBUGPORT=4005

set PORTOFFSET=0

set JMXCONFIG=-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=%JMXPORT% -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false

set ZOOKEEPERADMINCONFIG=-Dzookeeper.admin.serverPort=%ZOOADMINPORT%

set CLASSPATH=-classpath ".;../lib/*;../modules/*"

set DEBUGCONFIG=-Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=%DEBUGPORT%,suspend=n -Dorg.singam.debug.port=%DEBUGPORT%

rem set GCCONFIG=-server -XX:PermSize=512m -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:ParallelGCThreads=20 -XX:ConcGCThreads=5 -XX:InitiatingHeapOccupancyPercent=45

rem set GCCONFIG=-server -Xms1G -Xmx1G -XX:+UseG1GC

set GCCONFIG=-XX:+UseZGC -XX:InitiatingHeapOccupancyPercent=80

IF EXIST %MDC_JAVA_HOME%\bin\java.exe (

"%MDC_JAVA_HOME%\bin\java" -version

"%MDC_JAVA_HOME%\bin\java" --enable-preview --add-opens java.base/jdk.internal.ref=ALL-UNNAMED --add-opens java.base/java.lang.reflect=ALL-UNNAMED --add-opens java.base/jdk.internal.reflect=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.lang.invoke=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util.concurrent.atomic=ALL-UNNAMED -Xms2G -Xmx2G %GCCONFIG% %DEBUGCONFIG% %CLASSPATH% -Djava.net.preferIPv4Stack=true com.github.mdc.tasks.scheduler.executor.standalone.EmbeddedSchedulersNodeLauncher

) ELSE (
 @echo on
 echo %MDC_JAVA_HOME% doesnot exists, please set JAVA_HOME environment variable with correct path.
 pause
)

