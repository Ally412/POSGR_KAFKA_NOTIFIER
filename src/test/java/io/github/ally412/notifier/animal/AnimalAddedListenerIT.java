package io.github.ally412.notifier.animal;

import io.github.ally412.notifier.messaging.Topics;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.time.Duration;
import java.time.LocalDate;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Feeds the listener the exact JSON the shelter publishes — hand-written here on purpose,
 * because the two apps share no code. If the shelter's wire format drifts, this notices.
 */
@SpringBootTest
@Testcontainers
class AnimalAddedListenerIT {

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

    @Test
    void appliesShelterJsonToTheReadModel() {
        publish(503, 10, """
                {"animalId":503,"name":"Rex","species":"DOG","breed":"Husky",\
                "status":"AVAILABLE","intakeDate":"2026-08-11"}""");

        Animal animal = await(503L);
        assertThat(animal.getName()).isEqualTo("Rex");
        assertThat(animal.getSpecies()).isEqualTo(Species.DOG);
        assertThat(animal.getBreed()).isEqualTo("Husky");
        assertThat(animal.getStatus()).isEqualTo(Status.AVAILABLE);
        assertThat(animal.getIntakeDate()).isEqualTo(LocalDate.of(2026, 8, 11));
        assertThat(animal.getVersion()).isEqualTo(10L);
    }

    @Test
    void ignoresFieldsTheShelterAddsLater() {
        publish(504, 11, """
                {"animalId":504,"name":"Bella","species":"CAT","breed":"Siamese",\
                "status":"SOCIALIZING","intakeDate":"2026-08-11","microchipId":"XYZ-1"}""");

        assertThat(await(504L).getName()).isEqualTo("Bella");
    }

    /** At-least-once delivery means the same event arrives twice; that must change nothing. */
    @Test
    void redeliveryOfTheSameEventChangesNothing() {
        String payload = """
                {"animalId":505,"name":"Barsik","species":"CAT","breed":"Siberian",\
                "status":"AVAILABLE","intakeDate":"2026-08-11"}""";
        publish(505, 12, payload);
        await(505L);

        publish(505, 12, payload);

        assertThat(animalRepository.findAll())
                .filteredOn(a -> a.getAnimalId().equals(505L))
                .hasSize(1);
        assertThat(await(505L).getVersion()).isEqualTo(12L);
    }

    /**
     * An older event overtaking a newer one must not resurrect stale data. This is what lets the
     * read model survive redelivery and reordering without depending on the transport.
     */
    @Test
    void olderEventDoesNotOverwriteNewerState() {
        publish(506, 20, """
                {"animalId":506,"name":"Mira","species":"DOG","breed":"Collie",\
                "status":"ADOPTED","intakeDate":"2026-08-11"}""");
        await(506L);

        publish(506, 19, """
                {"animalId":506,"name":"STALE","species":"DOG","breed":"STALE",\
                "status":"SOCIALIZING","intakeDate":"2020-01-01"}""");

        // Give the stale event time to be (not) applied.
        sleep(2000);
        Animal animal = await(506L);
        assertThat(animal.getName()).isEqualTo("Mira");
        assertThat(animal.getStatus()).isEqualTo(Status.ADOPTED);
        assertThat(animal.getVersion()).isEqualTo(20L);
    }

    private void publish(long animalId, long eventId, String payload) {
        ProducerRecord<String, Object> record =
                new ProducerRecord<>(Topics.ANIMAL_ADDED, String.valueOf(animalId), payload);
        record.headers().add("event-id", String.valueOf(eventId).getBytes(UTF_8));
        kafkaTemplate.send(record);
    }

    private Animal await(Long animalId) {
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        while (System.nanoTime() < deadline) {
            Optional<Animal> found = animalRepository.findById(animalId);
            if (found.isPresent()) {
                return found.get();
            }
            sleep(200);
        }
        throw new AssertionError("Animal " + animalId + " never reached the read model");
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
