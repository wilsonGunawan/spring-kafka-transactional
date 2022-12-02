package com.example.springkafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SpringKafkaApplication implements ApplicationRunner {

  public static void main(String[] args) {
    SpringApplication.run(SpringKafkaApplication.class, args);
  }

  @Autowired
  private KafkaService kafkaService;

  @Override
  public void run(ApplicationArguments args) throws Exception {
    kafkaService.send("test.topic", "test message", true);
  }
}
