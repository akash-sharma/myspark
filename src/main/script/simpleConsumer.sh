#!/bin/sh

export SPARK_MAJOR_VERSION=2

# Command to remove the old configuration from HDFS


# Execute below spark command
spark-submit \
 --conf spark.app.name=Simple_Kafka_Consumer \
 --driver-memory 512m \
 --num-executors 4 \
 --executor-cores 3 \
 --executor-memory 512m \
 --conf spark.driver.memoryOverhead=1024m \
 --conf spark.sql.files.ignoreCorruptFiles=true \
 --conf spark.yarn.executor.memoryOverhead=384m \
 --conf spark.sql.shuffle.partitions=12 \
 --conf spark.streaming.backpressure.enabled=true \
 --conf spark.sql.session.timeZone=UTC \
 --conf spark.io.compression.codec=lz4 \
 --conf spark.eventLog.enabled=false \
 --conf spark.eventLog.compress=true \
 --conf spark.yarn.maxAppAttempts=2 \
 --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2\
 --files hdfs:/user/my/logs/log4j.properties \
 --conf spark.executor.extraJavaOptions='-Dlog4j.configuration=log4j.properties -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=0 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false' \
 --conf "spark.executor.extraClassPath=/usr/hdp/2.6.5.1050-37/usr/lib/apache-log4j-extras-1.2.17.jar" --conf "spark.driver.extraClassPath=/usr/hdp/2.6.5.1050-37/usr/lib/apache-log4j-extras-1.2.17.jar" \
 --master yarn \
 --deploy-mode cluster \
 --driver-java-options "-Dlog4j.configuration='log4j.properties' -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=0 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false" \
 --class com.myspark.job.SimpleKafkaConsumer \
 myspark-jar-with-dependencies.jar &


sleep 40
echo "Completed"
