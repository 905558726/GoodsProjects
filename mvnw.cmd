@echo off
set "JAVA_HOME=C:\Program Files\Java\jdk-17"
set "MVNW_REPOURL=https://repo.maven.apache.org/maven2"
set "MVNW_VERBOSE=true"
set "MAVEN_HOME=D:\maven\apache-maven-3.6.1"

if exist "%MAVEN_HOME%\bin\mvn.cmd" (
    set "MVN_CMD=%MAVEN_HOME%\bin\mvn.cmd"
    "%MVN_CMD%" %* -Dmaven.repo.local=E:\.maven\local_maven
    goto :end
)

rem Fallback: use wrapper jar
"%JAVA_HOME%\bin\java" ^
  -Dmaven.multiModuleProjectDirectory="%CD%" ^
  -Dmaven.repo.local=E:\.maven\local_maven ^
  -jar "%CD%\.mvn\wrapper\maven-wrapper.jar" ^
  %*

:end
