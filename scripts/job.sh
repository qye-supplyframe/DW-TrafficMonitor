outputPath="/user/qye/adhoc_detect_system_outage/QA_traffic_alert"
jar="trafficAlert*-jar-with-dependencies.jar"

runMonth="2024-02"
spark3-submit \
--class com.supplyframe.ds.monthlyReference.main \
--deploy-mode client \
--driver-memory 4g \
--executor-memory=16g --executor-cores=4 \
$jar $runMonth $outputPath

runDate="2024-02-14"
spark3-submit \
--class com.supplyframe.ds.dailyAlert.main \
--deploy-mode client \
--driver-memory 4g \
--executor-memory=16g --executor-cores=4 \
$jar $runDate $outputPath


runDate="2024-02-14"
runHour="06"
spark3-submit \
--class com.supplyframe.ds.hourlyAlert.main \
--deploy-mode client \
--driver-memory 4g \
--executor-memory=16g --executor-cores=4 \
$jar $runDate $runHour $outputPath
