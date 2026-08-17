package io.github.ally412.notifier.messaging;

import io.github.ally412.notifier.animal.Animal;
import io.github.ally412.notifier.animal.AnimalRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The dead letter machinery, exercised rather than assumed. Until this class nothing proved a
 * bad message actually reaches a .DLT topic, or — more importantly — that the consumer keeps
 * working afterwards. A poison pill that stalled a partition would look exactly like silence.
 * <p>
 * Note where the failure happens here. The value deserializer is StringDeserializer, which
 * accepts any bytes, so poll() always succeeds; it is StringJacksonJsonMessageConverter that
 * fails, during listener dispatch. That is what makes DefaultErrorHandler able to see it at all.
 */
@SpringBootTest
@Testcontainers
class DeadLetterIT {

    @Container
    @ServiceConnection
    static ConfluentKafkaContainer kafka = new ConfluentKafkaContainer("confluentinc/cp-kafka:7.8.0");

    @Container
    @ServiceConnection
    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17");

    @Autowired
    KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    AnimalRepository animalRepository;

    @Autowired
    DeadLetters deadLetters;

    @Test
    void unreadableMessageIsParkedOnTheDeadLetterTopic() throws Exception {
        String garbage = "this is not json at all";

        publish(Topics.ANIMAL_ADDED, "601", 1, garbage);

        ConsumerRecord<String, String> parked = deadLetters.await(deadLetters.animalAdded, "601");
        // The original bytes, unchanged — that is what makes the DLT inspectable and replayable.
        assertThat(parked.value()).isEqualTo(garbage);
        // Headers explain what went wrong and where it came from.
        assertThat(parked.headers().lastHeader("kafka_dlt-original-topic")).isNotNull();
        assertThat(parked.headers().lastHeader("kafka_dlt-exception-message")).isNotNull();
    }

    /**
     * The point of the whole mechanism: one bad message must not block the ones behind it.
     * Without a working recoverer the offset would never advance and this good message,
     * published afterwards, would never be seen.
     */
    @Test
    void consumerKeepsWorkingAfterAPoisonPill() {
        publish(Topics.ANIMAL_ADDED, "602", 2, "{ definitely not valid json ");

        publish(Topics.ANIMAL_ADDED, "603", 3, """
                {"animalId":603,"name":"Survivor","species":"DOG","breed":"Mix",\
                "status":"AVAILABLE","intakeDate":"2026-08-17"}""");

        Animal animal = await(603L);
        assertThat(animal.getName()).isEqualTo("Survivor");
    }

    /**
     * Routing is computed from the source topic, so every consumed topic needs its own declared
     * dead letter topic. A health alert failure must not be filed under animal ones.
     */
    @Test
    void failuresAreRoutedToTheDeadLetterTopicOfTheirOwnSource() throws Exception {
        publish(Topics.HEALTH_ALERT, "604", 4, "not json either");

        ConsumerRecord<String, String> parked = deadLetters.await(deadLetters.healthAlert, "604");
        assertThat(parked.value()).isEqualTo("not json either");
    }

    private void publish(String topic, String key, long eventId, String payload) {
        ProducerRecord<String, Object> record = new ProducerRecord<>(topic, key, payload);
        record.headers().add("event-id", String.valueOf(eventId).getBytes(UTF_8));
        kafkaTemplate.send(record);
    }

    private Animal await(Long animalId) {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (System.nanoTime() < deadline) {
            Optional<Animal> found = animalRepository.findById(animalId);
            if (found.isPresent()) {
                return found.get();
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
        throw new AssertionError("Animal " + animalId + " never reached the read model — "
                + "the consumer is probably stuck on the poison pill");
    }

    @TestConfiguration
    static class TestConfig {
        @Bean
        DeadLetters deadLetters() {
            return new DeadLetters();
        }
    }

    /**
     * Reads the dead letter topics as raw records. Deliberately typed to String rather than an
     * event class: whatever landed here failed to convert, so asking for a converted type would
     * dead-letter it a second time.
     */
    static class DeadLetters {
        final BlockingQueue<ConsumerRecord<String, String>> animalAdded = new LinkedBlockingQueue<>();
        final BlockingQueue<ConsumerRecord<String, String>> healthAlert = new LinkedBlockingQueue<>();

        @KafkaListener(topics = Topics.ANIMAL_ADDED_DLT, groupId = "dead-letter-it")
        void onAnimalAddedDlt(ConsumerRecord<String, String> record) {
            animalAdded.add(record);
        }

        @KafkaListener(topics = Topics.HEALTH_ALERT_DLT, groupId = "dead-letter-it")
        void onHealthAlertDlt(ConsumerRecord<String, String> record) {
            healthAlert.add(record);
        }

        /**
         * Tests in this class share a broker and these queues, and every test in it deliberately
         * produces a dead letter — so the head of the queue is very often somebody else's. Drain
         * until the expected key turns up rather than trusting arrival order.
         */
        ConsumerRecord<String, String> await(BlockingQueue<ConsumerRecord<String, String>> queue,
                                             String key) throws InterruptedException {
            List<String> seen = new ArrayList<>();
            long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (System.nanoTime() < deadline) {
                ConsumerRecord<String, String> record = queue.poll(500, TimeUnit.MILLISECONDS);
                if (record == null) {
                    continue;
                }
                if (key.equals(record.key())) {
                    return record;
                }
                seen.add(record.key());
            }
            throw new AssertionError("Nothing dead-lettered with key %s within 20s. Saw: %s"
                    .formatted(key, seen));
        }
    }
}
