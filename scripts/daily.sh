#!/bin/bash

set -e

PROJECT_PATH="/var/lib/etl/projects/DW-TrafficMonitor"
BASE_HDFS_PATH="/prod/etl/TrafficAlerts/SystemOutages"

EMAIL_LIST="etl@supplyframe.com"

JAR="${PROJECT_PATH}/target/trafficAlert-1.0.1-jar-with-dependencies.jar"

RUN_DATE=${1:-$(date -d yesterday +%Y-%m-%d)}
RUN_MONTH=$(date -d "${RUN_DATE}" +%Y-%m)

spark3-submit \
--class com.supplyframe.ds.monthlyReference.main \
--deploy-mode client \
--driver-memory 4g \
--executor-memory=16g --executor-cores=4 \
$JAR $RUN_MONTH $BASE_HDFS_PATH

spark3-submit \
--class com.supplyframe.ds.dailyAlert.main \
--deploy-mode client \
--driver-memory 4g \
--executor-memory=16g --executor-cores=4 \
$JAR $RUN_DATE $BASE_HDFS_PATH $EMAIL_LIST