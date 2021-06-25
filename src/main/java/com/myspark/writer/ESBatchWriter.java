package com.myspark.writer;

import com.myspark.job.SimpleKafkaConsumer;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ESBatchWriter extends ForeachWriter<Row> {

  private static final long serialVersionUID = 568213458L;
  private static Logger LOGGER = LoggerFactory.getLogger(SimpleKafkaConsumer.class);

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

    List<Map<String, Object>> recordList =
        partitionIdByBatchedPacketMap.get(TaskContext.getPartitionId());

    Map<String, Object> record = new LinkedHashMap<>();
    // TODO : add to record
    recordList.add(record);

    // send batch packets
    if (recordList.size() >= BATCH_COUNT) {
      RestHighLevelClient restHighLevelClient = ESClientUtils.getInstance();
      BulkRequest bulkRequest = new BulkRequest();
      // TODO : add to bulk request
      try {
        BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        LOGGER.info(
            "After request took ms :"
                + bulkResponse.getTook()
                + " docs fails:"
                + bulkResponse.hasFailures()
                + " with msg"
                + bulkResponse.buildFailureMessage());

        BulkItemResponse[] allResponses = bulkResponse.getItems();
        for (int i = 0; i < allResponses.length; i++) {

          LOGGER.info(
              "record indexed with : "
                  + allResponses[i].getIndex()
                  + " type : "
                  + allResponses[i].getType()
                  + " id : "
                  + allResponses[i].getId()
                  + " version : "
                  + allResponses[i].getFailure()
                  + " with op"
                  + allResponses[i].getOpType());
        }
        if (bulkResponse.hasFailures()) {
          throw new ElasticsearchException("ES batch Indexing has failed");
        }
      } catch (IOException e) {
        LOGGER.error("ES batch operation failed", e);
        throw new ElasticsearchException("ES batch operation failed", e);
      }
    }
    recordList.clear();
  }

  @Override
  public void close(Throwable throwable) {
    List<Map<String, Object>> batchedPackets =
        partitionIdByBatchedPacketMap.get(TaskContext.getPartitionId());
    batchedPackets.clear();
  }
}
