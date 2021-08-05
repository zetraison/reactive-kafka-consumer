package com.zetraison.reactivekafkaconsumer.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zetraison.reactivekafkaconsumer.dto.UserMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ParallelFlux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;

import java.time.Duration;
import java.util.function.Function;

public class KafkaMessageWorker {
    Logger log = LoggerFactory.getLogger(KafkaMessageWorker.class);

    private final KafkaReceiver<String, String> kafkaReceiver;
    
    private final Scheduler workerScheduler;

    private final int workerThreadsCount;

    private final ObjectMapper objectMapper;

    public KafkaMessageWorker(
            KafkaReceiver<String, String> kafkaReceiver,
            int workerThreadsCount,
            ObjectMapper objectMapper
    ) {
        this.kafkaReceiver = kafkaReceiver;
        this.workerScheduler = Schedulers.newParallel("worker-scheduler", workerThreadsCount);
        this.workerThreadsCount = workerThreadsCount;
        this.objectMapper = objectMapper;
    }

    @EventListener(ContextRefreshedEvent.class)
    private void consume() {
        kafkaReceiver
                .receiveAutoAck()
                .metrics()
                // Flux batch to Flux records
                .flatMap(batch -> batch)
                // BACKPRESSURE
                .delayElements(Duration.ofMillis(100L))
                .parallel(workerThreadsCount)
                .runOn(workerScheduler)
                .doOnNext(record -> log.info("received key={}, value={} from topic={}, offset={}",
                        record.key(),
                        record.value(),
                        record.topic(),
                        record.offset())
                )
                .transform(getDeserializeValueTransformConsumerRecord())
                .sequential()
                .doOnNext(userMessage -> log.info("successfully consumed {}={}", UserMessage.class.getSimpleName(), userMessage))
                .doOnError(throwable -> log.error("something bad happened while consuming : {}", throwable.getMessage()))
                .subscribe();
    }

    private Function<ParallelFlux<ConsumerRecord<String, String>>, ParallelFlux<UserMessage>> getDeserializeValueTransformConsumerRecord() {
        return inputFlux -> inputFlux.flatMap(record -> {
            try {
                return Mono.just(objectMapper.readValue(
                        record.value(),
                        new TypeReference<UserMessage>() {
                        }));
            } catch (Exception ex) {
                log.error(ex.getMessage());
            }
            return Mono.empty();
        });

    }
}
