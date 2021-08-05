package com.zetraison.reactivekafkaconsumer.service;

import com.zetraison.reactivekafkaconsumer.dto.UserMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;

public class KafkaMessageWorker {
    Logger log = LoggerFactory.getLogger(KafkaMessageWorker.class);

    private final ReactiveKafkaConsumerTemplate<String, UserMessage> reactiveKafkaConsumerTemplate;
    
    private final Scheduler workerScheduler;

    private final int workerThreadsCount;

    public KafkaMessageWorker(
            ReactiveKafkaConsumerTemplate<String, UserMessage> reactiveKafkaConsumerTemplate,
            int workerThreadsCount
    ) {
        this.reactiveKafkaConsumerTemplate = reactiveKafkaConsumerTemplate;
        this.workerScheduler = Schedulers.newParallel("worker-scheduler", workerThreadsCount);
        this.workerThreadsCount = workerThreadsCount;
    }

    @EventListener(ContextRefreshedEvent.class)
    private void consume() {
        reactiveKafkaConsumerTemplate
                .receiveAutoAck()
                .metrics()
                .parallel(workerThreadsCount)
                .runOn(workerScheduler)
                .sequential()
                .delayElements(Duration.ofMillis(10000L)) // BACKPRESSURE
                .doOnNext(record -> log.info("received key={}, value={} from topic={}, offset={}",
                        record.key(),
                        record.value(),
                        record.topic(),
                        record.offset())
                )
                .map(ConsumerRecord::value)
                .doOnNext(userMessage -> log.info("successfully consumed {}={}", UserMessage.class.getSimpleName(), userMessage))
                .doOnError(throwable -> log.error("something bad happened while consuming : {}", throwable.getMessage()))
                .subscribe();
    }
}
