package com.kafka.kafka_message_reprocess_api.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.kafka_message_reprocess_api.util.CustomAbstractConsumerSeekAware;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

@Slf4j
@Component
public class KafkaDemoOneListener extends CustomAbstractConsumerSeekAware {

    public final ObjectMapper mapper;

    public KafkaDemoOneListener(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @KafkaListener(groupId = "${kafka.consumer.demo.group-id}",
            topics = "#{'${kafka.consumer.topics}'.split(',')}",
            containerFactory = "kafkaListenerContainerFactory",
            idIsGroup = true, concurrency = "2")
    public void consumeDemoMessage(@Payload String message,
                                   @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                   @Header(KafkaHeaders.OFFSET) long offset,
                                   @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timestamp,
                                   @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                                   Acknowledgment acknowledgment) {
        try {

            ZonedDateTime offsetTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
            log.info("Message received in Topic: {}, Partition: {}, Offset: {},  timestamp: {}", topic, partition, offset, offsetTime);
            log.info("Message received : {}", message);
//            DummyModel dummyModel = mapper.readValue(message, DummyModel.class);
//            log.info("Dummy model : {}", dummyModel);

        } catch (Exception jpe) {
            log.error("Error: ", jpe);
        }
        acknowledgment.acknowledge();
    }

    @KafkaListener(groupId = "${kafka.consumer2.demo.group-id}",
            topics = "#{'${kafka.consumer2.topics}'.split(',')}",
            containerFactory = "kafkaListenerContainerFactory",
            idIsGroup = true, concurrency = "2")
    public void consumeDemo2Message(@Payload String message,
                                   @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                   @Header(KafkaHeaders.OFFSET) long offset,
                                   @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timestamp,
                                   @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                                   Acknowledgment acknowledgment) {
        try {
            ZonedDateTime offsetTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
            log.info("Message received in Topic: {}, Partition: {}, Offset: {},  timestamp: {}", topic, partition, offset, offsetTime);
            log.info("Message received : {}", message);
        } catch (Exception jpe) {
            log.error("Error: ", jpe);
        }
        acknowledgment.acknowledge();
    }

}