package io.github.ally412.notifier.messaging;

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
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.support.converter.StringJacksonJsonMessageConverter;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.Map;

@Configuration
@Slf4j
public class KafkaErrorHandlingConfig {
    @Value("${shelter.kafka.replicas:1}")
    private short replicas;
    @Bean
    public CommonErrorHandler commonErrorHandler(KafkaTemplate<String, Object> kafkaTemplate) {
        // Derived from the source topic rather than named outright: with more than one topic
        // consumed, a fixed destination would file health alert failures under animal ones.
        // Same partition as the source, which is why each dead letter topic is declared with
        // as many partitions as the topic it serves.
        DeadLetterPublishingRecoverer publisher =  new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, exception) ->
                        new TopicPartition(record.topic() + Topics.DLT_SUFFIX, record.partition()));

        return new DefaultErrorHandler((record, exception) -> {
            logUnreadableRecord(record, exception);
            publisher.accept(record, exception);
        }, new FixedBackOff(0L, 0L));
    }
    /**
     * Decides the payload type per listener method rather than per consumer factory: the
     * converter reads the target type from the @KafkaListener signature, so one factory can
     * serve AnimalAdded on one topic and HealthAlert on another.
     */
    @Bean
    public RecordMessageConverter messageConverter() {
        return new StringJacksonJsonMessageConverter();
    }

    @Bean
    public NewTopic animalAddedDeadLetterTopic() {
        return deadLetterTopic(Topics.ANIMAL_ADDED_DLT);
    }

    @Bean
    public NewTopic healthAlertDeadLetterTopic() {
        return deadLetterTopic(Topics.HEALTH_ALERT_DLT);
    }

    @Bean
    public NewTopic adoptionCompletedDeadLetterTopic() {
        return deadLetterTopic(Topics.ADOPTION_COMPLETED_DLT);
    }

    private NewTopic deadLetterTopic(String name) {
        return TopicBuilder.name(name).partitions(3).replicas(replicas).build();
    }


    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<?, ?> producerFactory) {
        // Only the types this app actually publishes: raw JSON text, which is what the tests
        // feed the listener, and bytes, for anything republished to a dead letter topic
        // verbatim. DelegatingByTypeSerializer needs an entry per type sent, and no more.
        Map<Class<?>, Serializer<?>> delegates = Map.of(
                byte[].class, new ByteArraySerializer(),
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
                record.key(),
                // The value is plain text now that conversion happens after deserialization,
                // so the payload is on the record itself rather than buried in the exception.
                record.value(),
                NestedExceptionUtils.getMostSpecificCause(exception).getMessage());
    }
}
