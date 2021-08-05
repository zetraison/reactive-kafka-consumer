package com.zetraison.reactivekafkaconsumer.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zetraison.reactivekafkaconsumer.service.KafkaMessageWorker;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.internals.ConsumerFactory;
import reactor.kafka.receiver.internals.DefaultKafkaReceiver;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@Configuration
@ConditionalOnProperty(prefix = "kafka.consumer", name = "enabled", havingValue = "true")
public class KafkaConsumerConfig {
    @Value(value = "${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value(value = "#{'${kafka.topics}'.split(',')}")
    private Collection<String> topicNames;

    @Value(value = "${kafka.consumer-group-id}")
    private String consumerGroupId;

    @Value(value = "${kafka.max-poll-records}")
    private Integer maxPollRecords;

    @Value(value = "${kafka.max-poll-interval-ms}")
    private Integer maxPollIntervalMs;

    @Value(value = "${kafka.atmost-once-commit-ahead-size}")
    private Integer atmostOnceCommitAheadSize;

    @Value(value = "${kafka.commit-batch-size}")
    private Integer commitBatchSize;

    @Value(value = "${kafka.commit-interval-millis}")
    private Integer commitIntervalMillis;

    @Value(value = "${kafka.auto-offset-reset}")
    private String autoOffsetReset;

    @Value("${kafka.consumer.worker.threads.count}")
    private int workerThreadsCount;

    @Bean
    public KafkaReceiver<String, String> kafkaReceiver() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        configProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        configProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollIntervalMs);
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);

        return new DefaultKafkaReceiver<>(
                ConsumerFactory.INSTANCE,
                ReceiverOptions.<String, String>create(configProps)
                        .atmostOnceCommitAheadSize(atmostOnceCommitAheadSize)
                        .commitBatchSize(commitBatchSize)
                        .commitInterval(Duration.ofMillis(commitIntervalMillis))
                        .subscription(topicNames)
        );
    }

    @Bean
    public KafkaMessageWorker kafkaMessageWorker(
            KafkaReceiver<String, String> reactiveKafkaReceiver,
            ObjectMapper objectMapper
    ) {
        return new KafkaMessageWorker(reactiveKafkaReceiver, workerThreadsCount, objectMapper);
    }
}
