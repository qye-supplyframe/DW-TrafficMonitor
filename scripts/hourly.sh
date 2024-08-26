#!/bin/bash

set -e

PROJECT_PATH="/var/lib/etl/projects/DW-TrafficMonitor"
BASE_HDFS_PATH="/prod/etl/TrafficAlerts/SystemOutages"

EMAIL_LIST="etl@supplyframe.com"

JAR="${PROJECT_PATH}/target/trafficAlert-1.0.1-jar-with-dependencies.jar"

RUN_DATE=${1:-$(date -d "-2 hour" +%Y-%m-%d)}
RUN_HOUR=${2:-$(date -d "-2 hour" +%H)}

spark3-submit \
--class com.supplyframe.ds.hourlyAlert.main \
--deploy-mode client \
--driver-memory 4g \
--executor-memory=16g --executor-cores=4 \
$JAR $RUN_DATE $RUN_HOUR $BASE_HDFS_PATH $EMAIL_LIST