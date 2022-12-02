package com.example.springkafka;

public interface KafkaService {

  void send(String topic, String message, boolean success);
}
