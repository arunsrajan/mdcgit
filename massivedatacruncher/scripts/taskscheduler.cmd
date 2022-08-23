title MDC Task Scheduler

@echo off

setLocal EnableDelayedExpansion

echo STARTING DataCruncher....

SET DATACRUNCHER=%~dp0\\..

SET DATACRUNCHERLIB=%DATACRUNCHER%\lib

SET DATACRUNCHERMODULES=%DATACRUNCHER%\modules

set JMXPORT=33330

set DEBUGPORT=4000

set PORTOFFSET=0

set JMXCONFIG=-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=%JMXPORT% -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false

set ZOOKEEPERADMINCONFIG=-Dzookeeper.admin.serverPort=%ZOOADMINPORT%

set CLASSPATH=-classpath ".;../lib/*;../modules/*"

set DEBUGCONFIG=-Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=%DEBUGPORT%,suspend=n -Dorg.singam.debug.port=%DEBUGPORT%

set MEMCONFIG=-Xms1G -Xmx1G

set GCCONFIG=-XX:+UseG1GC -XX:+CMSClassUnloadingEnabled -Dsun.rmi.dgc.client.gcInterval=3600000

IF EXIST %MDC_JAVA_HOME%\bin\java.exe (

"%MDC_JAVA_HOME%\bin\java" --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.lang.invoke=ALL-UNNAMED %DEBUGCONFIG% %CLASSPATH% %MEMCONFIG% %GCCCONFIG% -Djava.net.preferIPv4Stack=true com.github.mdc.tasks.scheduler.TaskSchedulerRunner

) ELSE (
 @echo on
 echo %MDC_JAVA_HOME% doesnot exists, please set JAVA_HOME environment variable with correct path.
 pause
)

