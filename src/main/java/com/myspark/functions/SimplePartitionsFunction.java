package com.myspark.functions;

import com.myspark.dto.SimpleConsumerOutputDto;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class SimplePartitionsFunction
    implements MapPartitionsFunction<Row, SimpleConsumerOutputDto> {

  private static final long serialVersionUID = 152985091510L;
  private static Logger LOGGER = LoggerFactory.getLogger(SimplePartitionsFunction.class);

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

    return resultList.iterator();
  }
}
