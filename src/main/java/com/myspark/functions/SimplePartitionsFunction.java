package com.myspark.functions;

import com.myspark.dto.SimpleConsumerOutputDto;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class SimplePartitionsFunction
    implements MapPartitionsFunction<Row, SimpleConsumerOutputDto> {

  private static final long serialVersionUID = 152985091510L;
  private static Logger LOGGER = LoggerFactory.getLogger(SimplePartitionsFunction.class);

  private static final DateTimeFormatter gmtFormatter = DateTimeFormatter.ISO_DATE_TIME;

  public SimplePartitionsFunction() {

    String threadName = Thread.currentThread().getName();
    LOGGER.info("Creating SimplePartitionsFunction for Thread : {}", threadName);
  }

  @Override
  public Iterator<SimpleConsumerOutputDto> call(Iterator<Row> input) throws Exception {

    List<SimpleConsumerOutputDto> resultList = new LinkedList<>();

    if (input == null) {
      return resultList.iterator();
    }
    while (input.hasNext()) {
      Row row = input.next();
      LOGGER.info("SimpleConsumer input row: {}", row);

      String customerId = row.getString(row.fieldIndex("customer_id"));
      Integer custIdAsInt = Integer.parseInt(customerId);
      String orderId = row.getString(row.fieldIndex("order_id"));
      String createdAt = row.getString(row.fieldIndex("created_at"));
      String client = row.getString(row.fieldIndex("client"));
      String packetProcessingTime = ZonedDateTime.now(ZoneId.of("GMT")).format(gmtFormatter);

      SimpleConsumerOutputDto simpleConsumerOutputDto =
          new SimpleConsumerOutputDto(
              custIdAsInt, orderId, createdAt, client, packetProcessingTime);
      LOGGER.info("simpleConsumerOutputDto : {}", simpleConsumerOutputDto);

      resultList.add(simpleConsumerOutputDto);
    }

    return resultList.iterator();
  }
}
