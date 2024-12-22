package com.kafka.kafka_message_reprocess_api.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Objects;

@Slf4j
@Component
public class ConsumerSeekAware {

    private final BeanFactory context;

    public ConsumerSeekAware(BeanFactory context) {
        this.context = context;
    }

    public void seekToBeginning(String listener, String topic, int partition) {
        CustomAbstractConsumerSeekAware offsetSeek = getCustomAbstractConsumerSeekAware(listener);
        try {
            Objects.requireNonNull(offsetSeek.fetchSeekCallbackFor(new TopicPartition(topic, partition)))
                    .seekToBeginning(topic, partition);
        } catch (NullPointerException npe) {
            log.error("seekToBeginning : Callback not found for topic: {}, partition: {}", topic, partition);
        } catch (Exception ex) {
            log.error("seekToBeginning : Error : ", ex);
        }
    }

    public void seekToBeginningAllPartitionOneTopic(String listener, String topic) {
        CustomAbstractConsumerSeekAware offsetSeek = getCustomAbstractConsumerSeekAware(listener);
        offsetSeek.fetchSeekCallbacks(topic).entrySet().stream()
                .filter(entry -> entry.getKey().topic().equalsIgnoreCase(topic))
                .forEach(entry -> {
                    int partition = entry.getKey().partition();
                    entry.getValue().seekToBeginning(topic, partition);
                });
    }

    public void seekToEnd(String listener, String topic, int partition) {
        CustomAbstractConsumerSeekAware offsetSeek = getCustomAbstractConsumerSeekAware(listener);
        try {
            Objects.requireNonNull(offsetSeek.fetchSeekCallbackFor(new TopicPartition(topic, partition)))
                    .seekToEnd(topic, partition);
        } catch (NullPointerException npe) {
            log.error("seekToEnd : Callback not found for topic: {}, partition: {}", topic, partition);
        } catch (Exception ex) {
            log.error("seekToEnd : Error : ", ex);
        }
    }

    public void seekToEndAllPartitionOneTopic(String listener, String topic) {
        CustomAbstractConsumerSeekAware offsetSeek = getCustomAbstractConsumerSeekAware(listener);
        offsetSeek.fetchSeekCallbacks(topic).entrySet().stream()
                .filter(entry -> entry.getKey().topic().equalsIgnoreCase(topic))
                .forEach(entry -> {
                    int partition = entry.getKey().partition();
                    entry.getValue().seekToEnd(topic, partition);
                });
    }

    public void seekToTimestamp(String listener, String topic, long timestamp) {
        CustomAbstractConsumerSeekAware offsetSeek = getCustomAbstractConsumerSeekAware(listener);
        Map<TopicPartition, org.springframework.kafka.listener.ConsumerSeekAware.ConsumerSeekCallback> topicPartitionConsumerSeekCallbackMap = offsetSeek.fetchSeekCallbacks(topic);
        for(Map.Entry<TopicPartition, org.springframework.kafka.listener.ConsumerSeekAware.ConsumerSeekCallback> entry : topicPartitionConsumerSeekCallbackMap.entrySet()) {

            entry.getValue().seekToTimestamp(List.of(entry.getKey()), timestamp);
        }
    }

    public void seekToTimestamp(String listener, TopicPartition topicPartition, long timestamp) {
        CustomAbstractConsumerSeekAware offsetSeek = getCustomAbstractConsumerSeekAware(listener);
        Map<TopicPartition, org.springframework.kafka.listener.ConsumerSeekAware.ConsumerSeekCallback> consumerSeekCallbackMap = offsetSeek.fetchSeekCallbacks(topicPartition.topic());
        org.springframework.kafka.listener.ConsumerSeekAware.ConsumerSeekCallback consumerSeekCallback = consumerSeekCallbackMap.get(topicPartition);
        consumerSeekCallback.seekToTimestamp(topicPartition.topic(), topicPartition.partition(), timestamp);
        //.entrySet().stream()
//                .filter(entry -> entry.getKey().topic().startsWith(topicPartition.topic()))
//                .forEach(entry -> entry.getValue().seekToTimestamp(List.of(topicPartition), timestamp));
    }

    public void seekRelativeBeginOrEndToTillOffset(String listener, String topic, int partition, long offset) {
        CustomAbstractConsumerSeekAware offsetSeek = getCustomAbstractConsumerSeekAware(listener);
        try {
            Objects.requireNonNull(offsetSeek.fetchSeekCallbackFor(new TopicPartition(topic, partition)))
                    .seekRelative(topic, partition, offset, false);
        } catch (NullPointerException npe) {
            log.error("seekRelativeBeginOrEndToTillOffset : Callback not found for topic: {}, partition: {}", topic, partition);
        } catch (Exception ex) {
            log.error("seekRelativeBeginOrEndToTillOffset : Error : ", ex);
        }
    }

    private CustomAbstractConsumerSeekAware getCustomAbstractConsumerSeekAware(String listenerClassName) {
        try {
            return (CustomAbstractConsumerSeekAware) this.context.getBean(Class.forName(listenerClassName));
        } catch (ClassNotFoundException e) {
            log.error("Error: ", e);
            throw new IllegalArgumentException(e.getMessage());
        }
    }

}
