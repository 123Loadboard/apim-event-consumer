@ECHO OFF
SETLOCAL

PUSHD "%~dp0"
SET ROOT=%CD%
POPD

SET JVMARGS=

SET JVMARGS=%JVMARGS% "-Djava.util.logging.config.file=%~dp0logger.properties"
SET JVMARGS=%JVMARGS% "-Deventhub.consumerName=%COMPUTERNAME%"

SET APPARGS=

:NextArg
IF "%~1" == "" GOTO Startup
SET ARG=%~1
IF "%ARG:~0,2%" == "-D" (
	SET JVMARGS=%JVMARGS% %1
) ELSE (
	SET APPARGS=%APPARGS% %1
)
SHIFT
GOTO NextArg

:ProcessArg

:Startup

@ECHO ON
java %JVMARGS% %CONFIG_PROPERTIES% -jar "%ROOT%\target\apim-event-consumer-0.0.1-SNAPSHOT.jar" %APPARGS%