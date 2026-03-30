#!/usr/bin/env bash
set -euo pipefail

echo "Checking for supported Java versions..."

JAVA_CANDIDATES=(
  "/usr/lib/jvm/java-17-openjdk-amd64"
  "/usr/lib/jvm/java-21-openjdk-amd64"
  "/usr/lib/jvm/java-17-openjdk"
  "/usr/lib/jvm/java-21-openjdk"
  "/usr/lib/jvm/temurin-17-jdk-amd64"
  "/usr/lib/jvm/temurin-21-jdk-amd64"
)

JAVA_HOME_FOUND=""

for candidate in "${JAVA_CANDIDATES[@]}"; do
  if [ -x "$candidate/bin/java" ]; then
    JAVA_HOME_FOUND="$candidate"
    break
  fi
done

if [ -z "$JAVA_HOME_FOUND" ]; then
  echo "ERROR: Java 17/21 not found."
  echo "Install one of them first, for example:"
  echo "  sudo apt-get update && sudo apt-get install -y openjdk-17-jdk"
  exit 1
fi

export JAVA_HOME="$JAVA_HOME_FOUND"
export PATH="$JAVA_HOME/bin:$PATH"

# Remove problematic option if it exists
unset JAVA_TOOL_OPTIONS || true

# Helpful for Spark in Codespaces / container environments
export SPARK_LOCAL_IP="127.0.0.1"
export SPARK_LOCAL_HOSTNAME="localhost"
export HADOOP_USER_NAME="codespace"

echo "Using JAVA_HOME=$JAVA_HOME"
java -version

echo "Running Spark historical process..."
python spark/process_historical.py

echo "Done."