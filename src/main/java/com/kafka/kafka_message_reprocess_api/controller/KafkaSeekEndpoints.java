package com.kafka.kafka_message_reprocess_api.controller;

import com.kafka.kafka_message_reprocess_api.model.TimestampSeekingModel;
import com.kafka.kafka_message_reprocess_api.util.ConsumerSeekAware;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.*;
import java.time.Instant;

@Slf4j
@RestController
@ConditionalOnProperty(name = "kafka.seek.endpoints.enabled", havingValue = "true", matchIfMissing = false)
public class KafkaSeekEndpoints {

    private final ConsumerSeekAware offsetReprocess;

    public KafkaSeekEndpoints(ConsumerSeekAware offsetReprocess) {
        this.offsetReprocess = offsetReprocess;
    }


    @Operation(description = "Reprocess messages from the beginning across all partitions of a topic in Kafka.")
    @ApiResponse(responseCode = "200", description = "The topic listener can start consuming messages from the beginning of a Kafka topic")
    @GetMapping("/listeners/{listenerName}/seek-from-begin/{topic}")
    public void seekToBeginningParticularTopic(@PathVariable String listenerName, @PathVariable String topic) {
        offsetReprocess.seekToBeginningAllPartitionOneTopic(listenerName, topic);
    }

    @Operation(description = "Reprocess messages from the start of a specific partition in a topic")
    @ApiResponse(responseCode = "200", description = "The topic listener can start consuming messages from the beginning of a specific partition in Kafka")
    @GetMapping("/listeners/{listenerName}/seek-from-begin/{topic}/{partition}")
    public void seekToBeginningParticularTopicPartition(@PathVariable String listenerName, @PathVariable String topic,
                                                        @PathVariable Integer partition) {
        offsetReprocess.seekToBeginning(listenerName, topic, partition);
    }

    @Operation(description = "Reprocess messages from the latest offset across all partitions of a topic")
    @ApiResponse(responseCode = "200", description = "The topic listener can consume messages starting from the latest offset available in Kafka")
    @GetMapping("/listeners/{listenerName}/seek-from-end/{topic}")
    public void seekToEndParticularTopic(@PathVariable String listenerName,
                                         @PathVariable String topic) {
        offsetReprocess.seekToEndAllPartitionOneTopic(listenerName, topic);
    }

    @Operation(description = "Reprocess messages from the latest offset in a specific partition of a topic")
    @ApiResponse(responseCode = "200", description = "The topic listener can consume messages from the latest offset available for a specific partition in Kafka")
    @GetMapping("/listeners/{listenerName}/seek-from-end/{topic}/{partition}")
    public void seekToEndParticularTopicPartition(@PathVariable String listenerName,
                                                  @PathVariable String topic,
                                                  @PathVariable Integer partition) {
        offsetReprocess.seekToEnd(listenerName, topic, partition);
    }

    @Operation(description = "Reprocess messages from a given timestamp in a specific Kafka topic")
    @ApiResponse(responseCode = "200", description = "The topic listener can start consuming messages from a specific timestamp in Kafka")
    @PostMapping("/listeners/{listenerName}/seek-by-timestamp")
    public void seekToBeginningParticularTopic(@PathVariable String listenerName, @RequestBody TimestampSeekingModel message) {
        Instant instant = message.getTimestamp().toInstant();
        if (null == message.getPartition()) {
            offsetReprocess.seekToTimestamp(listenerName, message.getTopic(), instant.toEpochMilli());
        } else {
            offsetReprocess.seekToTimestamp(listenerName, new TopicPartition(message.getTopic(),
                            message.getPartition()), instant.toEpochMilli());
        }
    }

    /**
     * @apiNote If offsetVal >= 0 from given OFFSET to END else(negative) from lastOffset - abs(offsetVal) to END
     * @param topic
     * @param partition
     * @param offset
     * **/
    @Operation(description = "Reprocess messages starting from a specified offset in a particular Kafka topic")
    @ApiResponse(responseCode = "200", description = "The topic listener can start consuming messages from a specific offset in Kafka")
    @GetMapping("/listeners/{listenerName}/seek-rel-given-offset/{topic}/{partition}/{offset}")
    public void seekRelativeGivenOffset(@PathVariable String listenerName,
                                        @PathVariable String topic,
                                        @PathVariable int partition,
                                        @PathVariable long offset) {
        offsetReprocess.seekRelativeBeginOrEndToTillOffset(listenerName, topic, partition, offset);
    }

}