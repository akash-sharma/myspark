package com.myspark.job;

import com.myspark.dto.SimpleConsumerOutputDto;
import com.myspark.functions.SimplePartitionsFunction;
import com.myspark.writer.ESBatchWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.SourceProgress;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryProgress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class SimpleKafkaConsumer {

  private static Logger LOGGER = LoggerFactory.getLogger(SimpleKafkaConsumer.class);

  public static void main(String args[]) {
    LOGGER.info("spark program started");

    SparkSession spark = null;
    try {

      spark = SparkSession.builder().getOrCreate();

      /*
       * -----------------------------------------------------------------------------
       * 1. kafka datasource
       * https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
       * -----------------------------------------------------------------------------
       */
      Map<String, String> kafkaOptions = new HashMap<>();
      kafkaOptions.put("kafka.bootstrap.servers", "localhost:9092");
      kafkaOptions.put("subscribePattern", "myTopicName");
      kafkaOptions.put("startingoffsets", "");
      kafkaOptions.put("kafka.max.partition.fetch.bytes", "10000");
      kafkaOptions.put(
          "spark.kafka.key.deserializer",
          "org.apache.kafka.common.serialization.StringDeserializer");
      kafkaOptions.put(
          "spark.kafka.value.deserializer",
          "org.apache.kafka.common.serialization.StringDeserializer");

      Dataset<Row> df = spark.readStream().format("kafka").options(kafkaOptions).load();
      df.printSchema();
      df =
          df.selectExpr(
              "CAST(key AS STRING) as key ",
              "CAST(value AS STRING) as value",
              "partition",
              "offset");
      df.createOrReplaceTempView("PIPELINE_1");
      df.printSchema();

      /*
       * -----------------------------------------------------------------------------
       * 2. kafka value, Stringified Json to columns
       * -----------------------------------------------------------------------------
       */
      String jsonTranformerSql =
          "SELECT cols.* from PIPELINE_1 P1  LATERAL VIEW "
              + " json_tuple(P1.value,'customer_id','order_id','created_at','client') cols "
              + " as customer_id,order_id,created_at,client WHERE value is not null ";

      Dataset<Row> stringToMapDs = spark.sql(jsonTranformerSql);
      stringToMapDs.createOrReplaceTempView("PIPELINE_2");
      stringToMapDs.printSchema();

      /*
       * -----------------------------------------------------------------------------
       * Sql column mapper and filter
       * -----------------------------------------------------------------------------
       */
      String columnMapperSql =
          "select customer_id,order_id,created_at,client "
              + " from PIPELINE_2 WHERE customer_id is not null AND order_id is not null ";

      Dataset<Row> sqlMapperFilterDs = spark.sql(columnMapperSql);
      sqlMapperFilterDs.createOrReplaceTempView("PIPELINE_3");
      sqlMapperFilterDs.printSchema();

      /*
       * -----------------------------------------------------------------------------
       * Data transformer
       * https://stackoverflow.com/questions/21185092/apache-spark-map-vs-mappartitions
       * -----------------------------------------------------------------------------
       */
      Dataset dataTransformerDs = spark.table("PIPELINE_3");
      Dataset mappedDs =
          dataTransformerDs.mapPartitions(
              new SimplePartitionsFunction(), Encoders.bean(SimpleConsumerOutputDto.class));
      mappedDs.createOrReplaceTempView("PIPELINE_4");
      mappedDs.printSchema();

      /*
       * -----------------------------------------------------------------------------
       * Data sync to ES
       * -----------------------------------------------------------------------------
       */
      DataStreamWriter<Row> dataStreamWriter = spark.table("PIPELINE_4").writeStream();
      ESBatchWriter writer = new ESBatchWriter();

      // when you want to write data on console
      // dataStreamWriter = dataStreamWriter.format("console");
      // dataStreamWriter = dataStreamWriter.option("truncate", "false");

      dataStreamWriter = dataStreamWriter.foreach(writer);
      StreamingQuery streamingQuery = dataStreamWriter.start();

      try {
        streamingQuery.awaitTermination(100);
      } catch (Exception e) {
        LOGGER.error("received exception : ", e);
        throw new RuntimeException("exception in awaitTermination", e);
      }

      StreamingQueryProgress streamingQueryProgress = streamingQuery.lastProgress();
      LOGGER.info("logger streamingQueryProgress : {}", streamingQueryProgress);
      System.out.println("sys streamingQueryProgress : " + streamingQueryProgress);
      if (streamingQueryProgress != null) {
        String consumerName = streamingQueryProgress.name();
        String timestamp = streamingQueryProgress.timestamp();
        SourceProgress[] sources = streamingQueryProgress.sources();

        Map<String, Long> durationMs = streamingQueryProgress.durationMs();
        Double inputRowsPerSecond = streamingQueryProgress.inputRowsPerSecond();
        Long inputRows = streamingQueryProgress.numInputRows();
        Double processedRowsPerSecond = streamingQueryProgress.processedRowsPerSecond();
      }

    } catch (Exception e) {
      LOGGER.error("error occurred in SimpleKafkaConsumer ", e);
    } finally {
      if (spark != null) {
        spark.close();
      }
    }
  }
}
