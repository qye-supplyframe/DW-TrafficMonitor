mvn clean package
scp target/trafficAlert_QA-*-jar-with-dependencies.jar us-lax-9a-ym-02.vpc.supplyframe.com:/home/qye/system_outage_detection
#scp scripts/job.sh us-lax-9a-ym-02.vpc.supplyframe.com:/home/qye/system_outage_detection
scp scripts/test_job.sh us-lax-9a-ym-02.vpc.supplyframe.com:/home/qye/system_outage_detection