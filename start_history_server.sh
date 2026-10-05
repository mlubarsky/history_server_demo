#!/usr/bin/env bash
# Starts the Spark History Server (bundled with pip's pyspark) at http://localhost:18080.
# It watches spark-events/ and turns every event log the jobs write into a browsable UI.
set -euo pipefail
DEMO_DIR="$(cd "$(dirname "$0")" && pwd)"
export JAVA_HOME="${JAVA_HOME:-$(/opt/homebrew/bin/brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home}"
export SPARK_HOME="$(python3 -c 'import pyspark, os; print(os.path.dirname(pyspark.__file__))')"
export SPARK_LOCAL_IP=127.0.0.1         # bind to localhost only, not your whole network
export SPARK_LOG_DIR="$DEMO_DIR/logs"    # history server's own log (not the apps' event logs)
export SPARK_PID_DIR="$DEMO_DIR/logs"
export SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=file://$DEMO_DIR/spark-events -Dspark.history.fs.update.interval=5s"
mkdir -p "$DEMO_DIR/spark-events" "$SPARK_LOG_DIR"
"$SPARK_HOME/sbin/start-history-server.sh"
echo "History server: http://localhost:18080  (log: $SPARK_LOG_DIR)"
