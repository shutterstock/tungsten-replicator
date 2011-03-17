@echo off
if "%OS%" == "Windows_NT" setlocal
rem Tungsten Replicator @VERSION@
rem (c) 2009 Continuent, Inc.  All rights reserved. 
rem
rem Replicator Windows start script
rem
rem Environmental variables required by this script: 
rem   REPLICATOR_HOME - Replicator release directory
rem   JAVA_HOME - Java release directory
rem
rem Additional environmental variables accepted by this script.  
rem   JVM_OPTIONS - Java VM options.  Defaults to -Xmx=256M. 
rem   REPLICATOR_LOG_DIR - Replicator log directory 
rem   REPLICATOR_RMI_PORT - Replicator RMI port
rem   REPLICATOR_CONF_DIR - Location of replicator conf dir
rem

rem Replicator manager class.
set RP_MGR_NAME="com.continuent.tungsten.replicator.management.ReplicationServiceManager"

rem
rem Set default for JVM_OPTIONS. 
rem
if not "%JVM_OPTIONS%" == "" goto JVM_OPTIONS_DEFINED
set JVM_OPTIONS=-Xmx256m
:JVM_OPTIONS_DEFINED

rem
rem Validate REPLICATOR_HOME. 
rem
if not "%REPLICATOR_HOME%" == "" goto REPLICATOR_HOME_DEFINED
echo REPLICATOR_HOME environmental variable must be defined
goto EXIT
:REPLICATOR_HOME_DEFINED
if exist "%REPLICATOR_HOME%\bin\trepstart.bat" goto REPLICATOR_HOME_OK
echo The REPLICATOR_HOME environment variable does not point to a valid release
goto EXIT
:REPLICATOR_HOME_OK

rem
rem Validate JAVA_HOME. 
rem
if not "%JAVA_HOME%" == "" goto JAVA_HOME_DEFINED
echo JAVA_HOME environmental variable must be defined
goto EXIT
:JAVA_HOME_DEFINED
if exist "%JAVA_HOME%\bin\java.exe" goto JAVA_HOME_OK
echo The JAVA_HOME environment variable does not point to a valid Java release
goto EXIT
:JAVA_HOME_OK

rem
rem Set CLASSPATH. 
rem
set CLASSPATH="%REPLICATOR_HOME%\conf"
for %%j in ("%REPLICATOR_HOME%\lib\*.jar") do call "%REPLICATOR_HOME%\bin\cpappend" "%%j"
for %%j in ("%REPLICATOR_HOME%\lib-ext\*.jar") do call "%REPLICATOR_HOME%\bin\cpappend" "%%j"

rem 
rem Set log directory location. 
rem
if not "%REPLICATOR_LOG_DIR%" == "" goto REPLICATOR_LOG_DIR_DEFINED
set REPLICATOR_LOG_DIR=%REPLICATOR_HOME%\log
:REPLICATOR_LOG_DIR_DEFINED
set JVM_OPTIONS=%JVM_OPTIONS% -Dreplicator.log.dir="%REPLICATOR_LOG_DIR%"

rem 
rem Set replicator conf dir location. 
rem
if "%REPLICATOR_CONF_DIR%" == "" goto REPLICATOR_PROPERTIES_OK
set JVM_OPTIONS=%JVM_OPTIONS% -Dreplicator.properties="%REPLICATOR_CONF_DIR%"
:REPLICATOR_PROPERTIES_OK

rem 
rem Set RMI port number. 
rem
if "%REPLICATOR_RMI_PORT%" == "" goto REPLICATOR_RMI_PORT_OK
set JVM_OPTIONS=%JVM_OPTIONS% -Dreplicator.rmi_port=%REPLICATOR_RMI_PORT%
:REPLICATOR_RMI_PORT_OK

rem Uncomment to debug replicator VM.
rem set REPLICATOR_JVMDEBUG_PORT=54002
rem set JVM_OPTIONS=%JVM_OPTIONS% -enableassertions -Xdebug -Xnoagent -Djava.compiler=none -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=%REPLICATOR_JVMDEBUG_PORT%

rem
rem Start Java VM. 
rem
echo on
"%JAVA_HOME%\bin\java" -cp %CLASSPATH% -Dcom.sun.management.jmxremote -Dreplicator.home.dir="%REPLICATOR_HOME%" %JVM_OPTIONS% %RP_MGR_NAME% %1 %2 %3 %4 %5 %6 %7 %8 %9
@echo off

:EXIT
if "%OS%" == "Windows_NT" endlocal
