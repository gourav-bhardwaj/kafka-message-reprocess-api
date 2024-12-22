package com.kafka.kafka_message_reprocess_api.util;

import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class CustomAbstractConsumerSeekAware extends AbstractConsumerSeekAware {

    public Map<TopicPartition, ConsumerSeekCallback> fetchSeekCallbacks(String topicName) {
        Map<TopicPartition, List<ConsumerSeekCallback>> topicsAndCallbacks = getTopicsAndCallbacks();
        return topicsAndCallbacks.entrySet().stream()
                .filter(entry -> entry.getKey().topic().equalsIgnoreCase(topicName))
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().get(0)));
    }

    public ConsumerSeekCallback fetchSeekCallbackFor(TopicPartition topicPartition) {
        List<ConsumerSeekAware.ConsumerSeekCallback> callbacks = getSeekCallbacksFor(topicPartition);
        return CollectionUtils.isEmpty(callbacks) ? null : (ConsumerSeekAware.ConsumerSeekCallback)callbacks.get(0);
    }

}
