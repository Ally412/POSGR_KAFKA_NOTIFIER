package io.github.ally412.notifier.adoption;

import io.github.ally412.notifier.animal.Animal;
import io.github.ally412.notifier.animal.AnimalRepository;
import io.github.ally412.notifier.animal.Status;
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
import java.util.function.Supplier;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Feeds the listener the exact JSON the shelter publishes — hand-written here on purpose,
 * because the two apps share no code. If the shelter's wire format drifts, this notices.
 * <p>
 * Unlike the other two events, AdoptionCompleted changes state an earlier event already wrote,
 * so these tests cover both read models at once.
 */
@SpringBootTest
@Testcontainers
class AdoptionListenerIT {

    @Container
    @ServiceConnection
    static ConfluentKafkaContainer kafka = new ConfluentKafkaContainer("confluentinc/cp-kafka:7.8.0");

    @Container
    @ServiceConnection
    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17");

    @Autowired
    KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    AdoptionRepository adoptionRepository;

    @Autowired
    AnimalRepository animalRepository;

    @Test
    void recordsTheAdoptionAndMovesTheAnimalToAdopted() {
        publishAnimalAdded(701, 40, """
                {"animalId":701,"name":"Rex","species":"DOG","breed":"Husky",\
                "status":"AVAILABLE","intakeDate":"2026-08-17"}""");
        awaitAnimal(701L);

        publishAdoption(701, 41, """
                {"animalId":701,"animalName":"Rex","adopterId":55,\
                "adopterName":"Anna Petrova","date":"2026-08-17"}""");

        Adoption adoption = await(() -> adoptionRepository.findById(701L), "adoption 701");
        assertThat(adoption.getAnimalName()).isEqualTo("Rex");
        assertThat(adoption.getAdopterId()).isEqualTo(55L);
        assertThat(adoption.getAdopterName()).isEqualTo("Anna Petrova");
        assertThat(adoption.getDate()).isEqualTo(LocalDate.of(2026, 8, 17));
        assertThat(adoption.getEventId()).isEqualTo(41L);
        assertThat(adoption.getReceivedAt()).isNotNull();

        // The other half of the same event: the animal itself moved.
        Animal animal = awaitStatus(701L, Status.ADOPTED);
        assertThat(animal.getVersion()).isEqualTo(41L);
    }

    @Test
    void ignoresFieldsTheShelterAddsLater() {
        publishAnimalAdded(702, 42, """
                {"animalId":702,"name":"Bella","species":"CAT","breed":"Siamese",\
                "status":"AVAILABLE","intakeDate":"2026-08-17"}""");
        awaitAnimal(702L);

        publishAdoption(702, 43, """
                {"animalId":702,"animalName":"Bella","adopterId":56,"adopterName":"Ivan Sidorov",\
                "date":"2026-08-17","adopterEmail":"ivan@example.com"}""");

        assertThat(await(() -> adoptionRepository.findById(702L), "adoption 702")
                .getAdopterName()).isEqualTo("Ivan Sidorov");
    }

    /** The relay re-sends a batch after a rollback, so the same adoption genuinely arrives twice. */
    @Test
    void redeliveryOfTheSameEventChangesNothing() {
        publishAnimalAdded(703, 44, """
                {"animalId":703,"name":"Barsik","species":"CAT","breed":"Siberian",\
                "status":"AVAILABLE","intakeDate":"2026-08-17"}""");
        awaitAnimal(703L);

        String payload = """
                {"animalId":703,"animalName":"Barsik","adopterId":57,\
                "adopterName":"Olga Kim","date":"2026-08-17"}""";
        publishAdoption(703, 45, payload);
        awaitStatus(703L, Status.ADOPTED);

        publishAdoption(703, 45, payload);

        sleep(2000);
        assertThat(adoptionRepository.findAll())
                .filteredOn(a -> a.getAnimalId().equals(703L))
                .hasSize(1);
        assertThat(animalRepository.findById(703L).orElseThrow().getVersion()).isEqualTo(45L);
    }

    /**
     * The version guard finally doing real work. Until this event existed, no two events ever
     * touched the same animal row, so the comparison in AnimalRepository.apply was unreachable:
     * a redelivered AnimalAdded must not undo an adoption that happened after it.
     */
    @Test
    void aRedeliveredAnimalAddedCannotUndoTheAdoption() {
        String animalAdded = """
                {"animalId":704,"name":"Mira","species":"DOG","breed":"Collie",\
                "status":"AVAILABLE","intakeDate":"2026-08-17"}""";
        publishAnimalAdded(704, 46, animalAdded);
        awaitAnimal(704L);

        publishAdoption(704, 47, """
                {"animalId":704,"animalName":"Mira","adopterId":58,\
                "adopterName":"Pavel Orlov","date":"2026-08-17"}""");
        awaitStatus(704L, Status.ADOPTED);

        // The relay re-sends the older event; its payload still says AVAILABLE.
        publishAnimalAdded(704, 46, animalAdded);

        sleep(2000);
        Animal animal = animalRepository.findById(704L).orElseThrow();
        assertThat(animal.getStatus()).isEqualTo(Status.ADOPTED);
        assertThat(animal.getVersion()).isEqualTo(47L);
    }

    private void publishAnimalAdded(long animalId, long eventId, String payload) {
        publish(Topics.ANIMAL_ADDED, animalId, eventId, payload);
    }

    private void publishAdoption(long animalId, long eventId, String payload) {
        publish(Topics.ADOPTION_COMPLETED, animalId, eventId, payload);
    }

    /** Keyed by animalId, as the shelter's outbox relay keys every one of these events. */
    private void publish(String topic, long animalId, long eventId, String payload) {
        ProducerRecord<String, Object> record =
                new ProducerRecord<>(topic, String.valueOf(animalId), payload);
        record.headers().add("event-id", String.valueOf(eventId).getBytes(UTF_8));
        kafkaTemplate.send(record);
    }

    private Animal awaitAnimal(Long animalId) {
        return await(() -> animalRepository.findById(animalId), "animal " + animalId);
    }

    private Animal awaitStatus(Long animalId, Status status) {
        return await(() -> animalRepository.findById(animalId).filter(a -> a.getStatus() == status),
                "animal " + animalId + " at status " + status);
    }

    private <T> T await(Supplier<Optional<T>> lookup, String what) {
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        while (System.nanoTime() < deadline) {
            Optional<T> found = lookup.get();
            if (found.isPresent()) {
                return found.get();
            }
            sleep(200);
        }
        throw new AssertionError(what + " never reached the read model");
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
