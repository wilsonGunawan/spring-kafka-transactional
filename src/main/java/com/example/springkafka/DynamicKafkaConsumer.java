package com.example.springkafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpoint;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.kafka.support.converter.MessageConverter;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.regex.Pattern;

@Component
@Slf4j
@RequiredArgsConstructor
public class DynamicKafkaConsumer {

  private final KafkaTemplate<String, Object> kafkaTemplate;

  private final KafkaListenerEndpointRegistry registry;

  public void startConsumer(String topic, String groupId) {
    ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(kafkaTemplate.getProducerFactory()
        .getConfigurationProperties()));
    factory.setConcurrency(1);

    ContainerProperties containerProperties = new ContainerProperties(topic);
    containerProperties.setGroupId(groupId);
    Properties properties = new Properties();
    properties.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
    properties.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
    containerProperties.setKafkaConsumerProperties(properties);
    KafkaMessageListenerContainer<String, String> container = new KafkaMessageListenerContainer<>(
        factory.getConsumerFactory(),
        containerProperties);

    MessageListener<String, String> messageListener = data -> log.info("Received message: {}",
        data.value());

    KafkaListenerEndpoint endpoint = new KafkaListenerEndpoint() {
      @Override
      public String getId() {
        return topic + "-" + groupId;
      }

      @Override
      public String getGroupId() {
        return groupId;
      }

      @Override
      public String getGroup() {
        return groupId;
      }

      @Override
      public Collection<String> getTopics() {
        return Collections.singletonList(topic);
      }

      @Override
      public TopicPartitionOffset[] getTopicPartitionsToAssign() {
        return new TopicPartitionOffset[0];
      }

      @Override
      public Pattern getTopicPattern() {
        return null;
      }

      @Override
      public String getClientIdPrefix() {
        return null;
      }

      @Override
      public Integer getConcurrency() {
        return 1;
      }

      @Override
      public Boolean getAutoStartup() {
        return true;
      }

      @Override
      public void setupListenerContainer(
          MessageListenerContainer listenerContainer, MessageConverter messageConverter) {
        container.setupMessageListener(messageListener);
      }

      @Override
      public boolean isSplitIterables() {
        return false;
      }
    };
    registry.registerListenerContainer(endpoint, factory);
    container.start();
  }

}
