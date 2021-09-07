title MDC Node Launcher

@echo off

setLocal EnableDelayedExpansion

echo STARTING DataCruncher....

SET DATACRUNCHER=%~dp0\\..

SET DATACRUNCHERLIB=%DATACRUNCHER%\lib

SET DATACRUNCHERMODULES=%DATACRUNCHER%\modules

set JMXPORT=33330

set DEBUGPORT=4001

set PORTOFFSET=0

set JMXCONFIG=-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=%JMXPORT% -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false

set ZOOKEEPERADMINCONFIG=-Dzookeeper.admin.serverPort=%ZOOADMINPORT%

set CLASSPATH=-classpath ".;../lib/*;../modules/*"

set DEBUGCONFIG=-Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=%DEBUGPORT%,suspend=n -Dorg.singam.debug.port=%DEBUGPORT%

set GCCONFIG=-server -Xms2G -Xmx2G -XX:+UnlockExperimentalVMOptions -XX:ConcGCThreads=4 -XX:G1NewSizePercent=10 -XX:G1MaxNewSizePercent=75 -XX:+UseG1GC
rem set GCCONFIG=-server -Xms2G -Xmx2G -XX:+UseG1GC -XX:+UnlockExperimentalVMOptions -XX:MaxGCPauseMillis=100 -XX:+DisableExplicitGC -XX:TargetSurvivorRatio=90 -XX:G1NewSizePercent=50 -XX:G1MaxNewSizePercent=80 -XX:G1MixedGCLiveThresholdPercent=35 -XX:+AlwaysPreTouch -XX:+ParallelRefProcEnabled

IF EXIST %MDC_JAVA_HOME%\bin\java.exe (

"%MDC_JAVA_HOME%\bin\java" --enable-preview %DEBUGCONFIG% %CLASSPATH% %GCCONFIG% -Djava.net.preferIPv4Stack=true com.github.mdc.tasks.executor.ContainerLauncher

) ELSE (
 @echo on
 echo %MDC_JAVA_HOME% doesnot exists, please set JAVA_HOME environment variable with correct path.
 pause
)

