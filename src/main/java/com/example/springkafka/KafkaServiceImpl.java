package com.example.springkafka;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class KafkaServiceImpl implements KafkaService {

  private final KafkaTemplate<String, String> kafkaTemplate;

  @Transactional
  @Override
  public void send(String topic, String message, boolean success) {
    kafkaTemplate.send(topic, message);

    if (!success) {
      throw new RuntimeException();
    }
  }
}
