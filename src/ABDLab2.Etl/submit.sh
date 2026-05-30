#!/usr/bin/env bash
set -euo pipefail

/opt/spark/bin/spark-submit \
  --master local[*] \
  --class org.apache.spark.deploy.dotnet.DotnetRunner \
  --packages org.postgresql:postgresql:42.7.4,com.clickhouse:clickhouse-jdbc:0.6.0,com.datastax.spark:spark-cassandra-connector_2.12:3.1.0 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --conf spark.cassandra.connection.host="${CASSANDRA_HOST:-cassandra}" \
  --conf spark.cassandra.connection.port="${CASSANDRA_PORT:-9042}" \
  /opt/microsoft-spark/microsoft-spark.jar \
  dotnet /app/ABDLab2.Etl.dll
