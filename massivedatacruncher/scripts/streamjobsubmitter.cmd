@echo off

title Stream Job Submitter

setLocal EnableDelayedExpansion

echo STARTING Stream Job Submitter....

SET DATACRUNCHER=%~dp0\\..

set CLASSPATH=-classpath ".;../lib/*;../modules/*"

set MEMCONFIG=-Xms128m -Xmx128m

set ADDOPENSMODULES=--enable-preview --add-opens java.base/java.math=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-modules jdk.incubator.foreign --add-opens java.base/jdk.internal.ref=ALL-UNNAMED --add-opens java.base/java.lang.reflect=ALL-UNNAMED --add-opens java.base/jdk.internal.reflect=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.lang.invoke=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens java.base/java.util.concurrent=ALL-UNNAMED --add-opens java.base/java.net=ALL-UNNAMED --add-opens java.base/java.text=ALL-UNNAMED --add-opens java.sql/java.sql=ALL-UNNAMED

set GCCONFIG=-XX:+UseZGC -XX:InitiatingHeapOccupancyPercent=80

IF EXIST %MDC_JAVA_HOME%\bin\java.exe (

"%MDC_JAVA_HOME%\bin\java" -version

"%MDC_JAVA_HOME%\bin\java" %MEMCONFIG% %ADDOPENSMODULES% %GCCONFIG% %DEBUGCONFIG% %CLASSPATH% -Djava.net.preferIPv4Stack=true com.github.mdc.stream.submitter.StreamPipelineJobSubmitter %*

pause
) ELSE (
 @echo on
 echo %MDC_JAVA_HOME% doesnot exists, please set JAVA_HOME environment variable with correct path.
 pause
)

