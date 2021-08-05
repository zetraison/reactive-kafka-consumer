package com.zetraison.reactivekafkaconsumer.config;

import com.zetraison.reactivekafkaconsumer.dto.UserMessage;
import com.zetraison.reactivekafkaconsumer.service.KafkaMessageWorker;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.Collections;

@Configuration
@ConditionalOnProperty(prefix = "kafka.consumer", name = "enabled", havingValue = "true")
public class KafkaConsumerConfig {
    @Value("${kafka.consumer.topic}")
    private String topic;

    @Value("${kafka.consumer.worker.threads.count}")
    private int workerThreadsCount;

    @Bean
    public ReceiverOptions<String, UserMessage> kafkaReceiverOptions(
            KafkaProperties kafkaProperties
    ) {
        ReceiverOptions<String, UserMessage> basicReceiverOptions = ReceiverOptions.create(kafkaProperties.buildConsumerProperties());
        return basicReceiverOptions.subscription(Collections.singletonList(topic));
    }

    @Bean
    public ReactiveKafkaConsumerTemplate<String, UserMessage> reactiveKafkaConsumerTemplate(
            ReceiverOptions<String, UserMessage> kafkaReceiverOptions
    ) {
        return new ReactiveKafkaConsumerTemplate<>(kafkaReceiverOptions);
    }

    @Bean
    public KafkaMessageWorker kafkaMessageWorker(
            ReactiveKafkaConsumerTemplate<String, UserMessage> reactiveKafkaConsumerTemplate
    ) {
        return new KafkaMessageWorker(
                reactiveKafkaConsumerTemplate,
                workerThreadsCount
        );
    }
}
