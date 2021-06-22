package com.myspark.writer;

import com.myspark.job.SimpleKafkaConsumer;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ESBatchWriter extends ForeachWriter<Row> {

  private static final long serialVersionUID = 568213458L;
  private static Logger LOGGER = LoggerFactory.getLogger(SimpleKafkaConsumer.class);

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
    if (recordList.size() >= 50) {
      // TODO : send packets in batch
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
