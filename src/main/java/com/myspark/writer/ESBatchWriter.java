package com.myspark.writer;

import com.myspark.util.DateUtil;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ESBatchWriter extends ForeachWriter<Row> {

  private static final long serialVersionUID = 568213458L;
  private static Logger LOGGER = LoggerFactory.getLogger(ESBatchWriter.class);

  private static final int BATCH_COUNT = 1; // 50

  private Map<Integer, List<Map<String, Object>>> partitionIdByBatchedPacketMap =
      new ConcurrentHashMap<>();

  @Override
  public boolean open(long l, long l1) {

    if (!partitionIdByBatchedPacketMap.containsKey(TaskContext.getPartitionId())) {
      partitionIdByBatchedPacketMap.put(TaskContext.getPartitionId(), new ArrayList<>());
    }
    return true;
  }

  @Override
  public void process(Row row) {

    LOGGER.info("row in ESBatchWriter {}", row);
    List<Map<String, Object>> recordList =
        partitionIdByBatchedPacketMap.get(TaskContext.getPartitionId());

    Map<String, Object> record = new HashMap<>();
    populateRecord(row, record);
    recordList.add(record);
    LOGGER.info("recordList : {}", recordList);

    // send batch packets
    if (recordList.size() >= BATCH_COUNT) {
      BulkRequest bulkRequest = new BulkRequest();
      for (Iterator iterator = recordList.iterator(); iterator.hasNext(); ) {
        Map<String, Object> recordMap = (Map<String, Object>) iterator.next();
        LOGGER.info("recordMap : {}", recordMap);
        bulkRequest.add(getIndexRequest(recordMap));
      }

      RestHighLevelClient restHighLevelClient = ESClientUtils.getInstance();
      try {
        BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        LOGGER.info(
            "ES request time in ms : {}, hasFailures : {}, buildFailureMessage : {}",
            bulkResponse.getTook(),
            bulkResponse.hasFailures(),
            bulkResponse.buildFailureMessage());
        LOGGER.info("bulkResponse : {}", bulkResponse);

        BulkItemResponse[] allResponses = bulkResponse.getItems();
        for (int index = 0; index < allResponses.length; index++) {

          BulkItemResponse itemResponse = allResponses[index];
          LOGGER.info(
              "Record indexed with : {}, type : {}, id : {}, failure : {}, operation : {}",
              itemResponse.getIndex(),
              itemResponse.getType(),
              itemResponse.getId(),
              itemResponse.getFailure(),
              itemResponse.getOpType());
          LOGGER.info("itemResponse : {}", itemResponse);
        }
        if (bulkResponse.hasFailures()) {
          throw new ElasticsearchException(
              "ES batch Indexing has failed with msg : " + bulkResponse.buildFailureMessage());
        }
      } catch (IOException e) {
        LOGGER.error("ES batch operation failed", e);
        throw new ElasticsearchException("ES batch operation failed", e);
      }
      recordList.clear();
    }
  }

  @Override
  public void close(Throwable throwable) {
    List<Map<String, Object>> batchedPackets =
        partitionIdByBatchedPacketMap.get(TaskContext.getPartitionId());
    batchedPackets.clear();
  }

  private void populateRecord(Row row, Map<String, Object> record) {

    StructType schema = row.schema();
    LOGGER.info("schema : {}", schema);
    StructField[] fields = schema.fields();
    LOGGER.info("fields : {}", fields);
    for (int index = 0; index < fields.length; index++) {
      String columnName = fields[index].name();
      Object value = row.get(index);
      if (value != null) {
        DataType dataType = fields[index].dataType();
        if (dataType == DataTypes.LongType || dataType == DataTypes.IntegerType) {
          record.put(columnName, ((Number) value).longValue());
        } else if (dataType == DataTypes.DoubleType || dataType == DataTypes.FloatType) {
          record.put(columnName, ((Number) value).doubleValue());
        } else if (dataType == DataTypes.StringType) {
          record.put(columnName, value.toString());
        } else if (dataType == DataTypes.TimestampType || dataType == DataTypes.DateType) {
          Long timestamp = row.getTimestamp(index).getTime();
          String zonedDateTime = DateUtil.getZonedDateTime(timestamp);
          record.put(columnName, zonedDateTime);
        } else {
          record.put(columnName, value);
        }
      } else {
        record.put(columnName, null);
      }
    }
  }

  private IndexRequest getIndexRequest(Map<String, Object> recordMap) {

    Long customerId = (Long) recordMap.get("customerId");
    String customerIdStr = customerId.toString();
    LOGGER.info("customerId : {}", customerId);
    TimeValue timeoutValue = TimeValue.timeValueSeconds(1);

    IndexRequest indexRequest = new IndexRequest("myindexname", "esTypeName", customerIdStr);
    indexRequest.source(recordMap);
    indexRequest.routing(customerIdStr);
    indexRequest.timeout(timeoutValue);
    return indexRequest;
  }
}
