package com.myspark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

public class App {

  public static void main(String args[]) {
    System.out.println("hello spark");

    SparkSession spark = SparkSession.builder().getOrCreate();

    // 1. kafka datasource
    // https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html

    Map<String, String> kafkaOptions = new HashMap<>();
    kafkaOptions.put("kafka.bootstrap.servers", "localhost:9092");
    kafkaOptions.put("subscribePattern", "myTopicName");
    kafkaOptions.put("startingoffsets", "");
    kafkaOptions.put("kafka.max.partition.fetch.bytes", "10000");
    kafkaOptions.put(
        "spark.kafka.key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
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
            "offset",
            "timestamp");

    df.createOrReplaceTempView("kafka_streaming_data_source");
    df.printSchema();

    // 2. kafka value, Stringified Json to columns

    String jsonTranformerSql =
        "SELECT cols.* from kafka_streaming_data_source t1  LATERAL VIEW json_tuple(t1.value,'user_id','code','site_id','updated_at','type','client','metadata') cols as user_id,code,site_id,updated_at,type,client,metadata WHERE value is not null ";

    Dataset<Row> stringToMapDs = spark.sql(jsonTranformerSql);

    stringToMapDs.createOrReplaceTempView("json_promo_record");
    stringToMapDs.printSchema();

    // Sql column mapper and filter

    String columnMapperSql =
        "select  user_id as userId ,  code ,  site_id ,  updated_at ,  type ,  client ,  metadata from json_promo_record WHERE code is not null AND site_id is not null ";

    Dataset<Row> sqlMapperFilterDs = spark.sql(columnMapperSql);

    sqlMapperFilterDs.createOrReplaceTempView("col_mapper");
    sqlMapperFilterDs.printSchema();

    // Data transformer
    // https://stackoverflow.com/questions/21185092/apache-spark-map-vs-mappartitions

    Dataset dataTransformerDs = spark.table("col_mapper");

    /*Dataset mappedDs = dataTransformerDs.mapPartitions(new MyVoucherFunctionByGroup(), Encoders.bean(MyVoucherPromoCodeOutputDTO.class));

    mappedDs.createOrReplaceTempView("enrich_promo_data");
    mappedDs.printSchema();*/

    // Data sync, cassandra, ES, kafka

  }
}
