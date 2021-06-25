package com.myspark.util;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class DateUtil {

  public static String getZonedDateTime(long timeOfEpoch) {

    Instant instant = Instant.ofEpochMilli(timeOfEpoch);
    ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(instant, ZoneId.of("GMT"));
    String zonedDateTimeString = zonedDateTime.format(DateTimeFormatter.ISO_DATE_TIME);
    return zonedDateTimeString;
  }
}
