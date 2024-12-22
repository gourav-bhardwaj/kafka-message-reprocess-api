package com.kafka.kafka_message_reprocess_api.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.time.ZonedDateTime;

@Getter
@Setter
@ToString
public class TimestampSeekingModel {

    private String topic;
    private Integer partition;
    private ZonedDateTime timestamp;

}
