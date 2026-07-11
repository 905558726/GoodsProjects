#!/usr/bin/env bash
# Maven Wrapper launcher
# Locate JAVA_HOME and Maven, then execute mvn

# Find JAVA_HOME for JDK 17
export JAVA_HOME="${JAVA_HOME:-C:/Program Files/Java/jdk-17}"
export PATH="$JAVA_HOME/bin:$PATH"

# Maven home
MAVEN_HOME="${MAVEN_HOME:-D:/maven/apache-maven-3.6.1}"
MVN_CMD="$MAVEN_HOME/bin/mvn"

# Default repo
REPO_LOCAL="${REPO_LOCAL:-E:/.maven/local_maven}"

if [ ! -f "$MVN_CMD" ]; then
    echo "[ERROR] Maven not found at $MVN_CMD"
    echo "Please set MAVEN_HOME environment variable"
    exit 1
fi

"$MVN_CMD" "$@" -Dmaven.repo.local="$REPO_LOCAL"
