package io.github.ally412.notifier;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.nio.charset.StandardCharsets;
import java.util.Map;

@Configuration
@Slf4j
public class KafkaErrorHandlingConfig {
    @Value("${shelter.kafka.replicas:1}")
    private short replicas;
    /**
     * A failed record is logged AND republished to shelter.animal.added.DLT. The log serves
     * whoever is watching the console now; the dead letter topic keeps the message itself, so
     * it can be inspected, counted and replayed once the cause is fixed.
     */
    @Bean
    public CommonErrorHandler commonErrorHandler(KafkaTemplate<String, Object> kafkaTemplate) {
        // The destination is resolved explicitly rather than left to the default, which appends
        // "-dlt" to the source topic name. Relying on that convention silently auto-created a
        // SECOND topic with one partition, so any failure on partition 1 or 2 would have had
        // nowhere to go. Naming it here keeps the constant authoritative for both the NewTopic
        // bean above and the publishing, and same-partition routing needs the 3 declared there.
        DeadLetterPublishingRecoverer publisher =  new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, exception) -> new TopicPartition(Topics.ANIMAL_ADDED_DLT, record.partition()));

        return new DefaultErrorHandler((record, exception) -> {
            logUnreadableRecord(record, exception);
            publisher.accept(record, exception);
        }, new FixedBackOff(0L, 0L));
    }
    @Bean
    public NewTopic animalAddedDeadLetterTopic() {
        return TopicBuilder.name(Topics.ANIMAL_ADDED_DLT).partitions(3).replicas(replicas).build();
    }


    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<?, ?> producerFactory) {
        Map<Class<?>, Serializer<?>> delegates = Map.of(
                byte[].class, new ByteArraySerializer(),
                AnimalAdded.class, new JacksonJsonSerializer<>(),
                String.class, new StringSerializer());

        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(
                producerFactory.getConfigurationProperties(),
                new StringSerializer(),
                new DelegatingByTypeSerializer(delegates)));
    }

    private void logUnreadableRecord(ConsumerRecord<?, ?> record, Exception exception) {
        log.error("Dropping unreadable record {}-{}@{} key={} payload={} — {}",
                record.topic(),
                record.partition(),
                record.offset(),
                record.key(),                 // the key still parses; only the value failed
                rawValueOf(exception),
                NestedExceptionUtils.getMostSpecificCause(exception).getMessage());
    }


    private String rawValueOf(Throwable exception) {
        for (Throwable cause = exception; cause != null; cause = cause.getCause()) {
            if (cause instanceof DeserializationException deserialization) {
                return new String(deserialization.getData(), StandardCharsets.UTF_8);
            }
            if (cause.getCause() == cause) {   // self-referencing cause: stop, or we loop forever
                break;
            }
        }
        return "<not a deserialization failure>";
    }
}
