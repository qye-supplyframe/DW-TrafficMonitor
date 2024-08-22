#!/bin/bash

# Define variables
#runMonth="2024-05"
#runDate="2024-03-01"
outputPath="/user/qye/adhoc_detect_system_outage/QA_traffic_alert"
jar="trafficAlert*-jar-with-dependencies.jar"
monthlyRefClass="com.supplyframe.ds.monthlyReference.main"
dailyAlertClass="com.supplyframe.ds.dailyAlert.main"
hourlyAlertClass="com.supplyframe.ds.hourlyAlert.main"

# Function to run spark3-submit and check the exit code
run_spark_job() {
    local jobClass=$1
    shift
    local args="$@"

    echo "Running: spark3-submit --class $jobClass --deploy-mode client --driver-memory 4g --executor-memory=16g --executor-cores=4 $jar $args"
    spark3-submit --class "$jobClass" --deploy-mode client --driver-memory 4g --executor-memory=16g --executor-cores=4 $jar $args
    local exitCode=$?
    if [ $exitCode -ne 0 ]; then
        echo "Error: Job failed with exit code $exitCode"
        exit $exitCode
    fi
}

# Test monthlyReference job for February 2024
echo "Testing monthlyReference job for February 2024..."
run_spark_job "$monthlyRefClass" "2024-01" "$outputPath"
run_spark_job "$monthlyRefClass" "2024-02" "$outputPath"
run_spark_job "$monthlyRefClass" "2024-03" "$outputPath"
run_spark_job "$monthlyRefClass" "2024-04" "$outputPath"
run_spark_job "$monthlyRefClass" "2024-05" "$outputPath"
run_spark_job "$monthlyRefClass" "2024-06" "$outputPath"


# Test hourlyAlert job for February 14 - February 15, 2024
echo "Testing hourlyAlert job from February 14 to February 17, 2024..."
run_spark_job "$dailyAlertClass" "2024-01-14" "$outputPath"
run_spark_job "$dailyAlertClass" "2024-02-01" "$outputPath"
run_spark_job "$dailyAlertClass" "2024-02-14" "$outputPath"
run_spark_job "$dailyAlertClass" "2024-02-15" "$outputPath"
run_spark_job "$dailyAlertClass" "2024-02-16" "$outputPath"
run_spark_job "$dailyAlertClass" "2024-03-01" "$outputPath"
run_spark_job "$dailyAlertClass" "2024-04-28" "$outputPath"
run_spark_job "$dailyAlertClass" "2024-05-01" "$outputPath"
run_spark_job "$dailyAlertClass" "2024-06-25" "$outputPath"

# Loop through each day and each hour
startDate="2024-01-14"
endDate="2024-01-15"

currentDate="$startDate"
while [[ "$currentDate" < "$endDate" ]]; do
    for hour in $(seq -w 0 23); do
        echo "Testing hourlyAlert job for $currentDate, hour $hour..."
        run_spark_job "$hourlyAlertClass" "$currentDate" "$hour" "$outputPath"
    done
    currentDate=$(date -I -d "$currentDate + 1 day")
done

startDate="2024-02-01"
endDate="2024-02-02"

currentDate="$startDate"
while [[ "$currentDate" < "$endDate" ]]; do
    for hour in $(seq -w 0 23); do
        echo "Testing hourlyAlert job for $currentDate, hour $hour..."
        run_spark_job "$hourlyAlertClass" "$currentDate" "$hour" "$outputPath"
    done
    currentDate=$(date -I -d "$currentDate + 1 day")
done

startDate="2024-02-14"
endDate="2024-02-15"

currentDate="$startDate"
while [[ "$currentDate" < "$endDate" ]]; do
    for hour in $(seq -w 0 23); do
        echo "Testing hourlyAlert job for $currentDate, hour $hour..."
        run_spark_job "$hourlyAlertClass" "$currentDate" "$hour" "$outputPath"
    done
    currentDate=$(date -I -d "$currentDate + 1 day")
done


startDate="2024-02-28"
endDate="2024-03-02"

currentDate="$startDate"
while [[ "$currentDate" < "$endDate" ]]; do
    for hour in $(seq -w 0 23); do
        echo "Testing hourlyAlert job for $currentDate, hour $hour..."
        run_spark_job "$hourlyAlertClass" "$currentDate" "$hour" "$outputPath"
    done
    currentDate=$(date -I -d "$currentDate + 1 day")
done

# Loop through each day and each hour
startDate="2024-05-01"
endDate="2024-05-02"

currentDate="$startDate"
while [[ "$currentDate" < "$endDate" ]]; do
    for hour in $(seq -w 0 23); do
        echo "Testing hourlyAlert job for $currentDate, hour $hour..."
        run_spark_job "$hourlyAlertClass" "$currentDate" "$hour" "$outputPath"
    done
    currentDate=$(date -I -d "$currentDate + 1 day")
done


echo "All tests completed successfully."
