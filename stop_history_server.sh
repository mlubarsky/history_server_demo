#!/usr/bin/env bash
set -euo pipefail
DEMO_DIR="$(cd "$(dirname "$0")" && pwd)"
export SPARK_HOME="$(python3 -c 'import pyspark, os; print(os.path.dirname(pyspark.__file__))')"
export SPARK_PID_DIR="$DEMO_DIR/logs"
"$SPARK_HOME/sbin/stop-history-server.sh"
