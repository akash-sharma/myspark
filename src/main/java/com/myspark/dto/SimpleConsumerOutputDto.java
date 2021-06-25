package com.myspark.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

public class SimpleConsumerOutputDto implements Serializable {

  private static final long serialVersionUID = 152988091560L;

  @JsonProperty("customer_id")
  private Integer customerId;

  @JsonProperty("order_id")
  private String orderId;

  @JsonProperty("created_at")
  private String createdAt;

  @JsonProperty("client")
  private String client;

  private String packetProcessingTime;

  public SimpleConsumerOutputDto() {}

  public SimpleConsumerOutputDto(
      Integer customerId,
      String orderId,
      String createdAt,
      String client,
      String packetProcessingTime) {
    this.customerId = customerId;
    this.orderId = orderId;
    this.createdAt = createdAt;
    this.client = client;
    this.packetProcessingTime = packetProcessingTime;
  }

  public Integer getCustomerId() {
    return customerId;
  }

  public String getOrderId() {
    return orderId;
  }

  public String getCreatedAt() {
    return createdAt;
  }

  public String getClient() {
    return client;
  }

  public String getPacketProcessingTime() {
    return packetProcessingTime;
  }

  @Override
  public String toString() {
    return "SimpleConsumerOutputDto{"
        + "customerId="
        + customerId
        + ", orderId='"
        + orderId
        + '\''
        + ", createdAt='"
        + createdAt
        + '\''
        + ", client='"
        + client
        + '\''
        + ", packetProcessingTime='"
        + packetProcessingTime
        + '\''
        + '}';
  }
}
