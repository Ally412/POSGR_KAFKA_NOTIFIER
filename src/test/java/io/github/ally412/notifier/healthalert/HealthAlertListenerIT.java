package io.github.ally412.notifier.healthalert;

import io.github.ally412.notifier.animal.Species;
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
 * <p>
 * Records are keyed by animal, not by medical record, exactly as the shelter keys them: one
 * animal's events share a partition, so an alert cannot overtake the AnimalAdded it belongs to.
 */
@SpringBootTest
@Testcontainers
class HealthAlertListenerIT {

    @Container
    @ServiceConnection
    static ConfluentKafkaContainer kafka = new ConfluentKafkaContainer("confluentinc/cp-kafka:7.8.0");

    @Container
    @ServiceConnection
    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17");

    @Autowired
    KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    HealthAlertRepository healthAlertRepository;

    @Test
    void appliesShelterJsonToTheReadModel() {
        publish(901, 603, 30, """
                {"medicalRecordId":901,"animalId":603,"animalName":"Rex","species":"DOG",\
                "breed":"Husky","urgency":"CRITICAL","description":"Suspected fracture, hind leg",\
                "vetName":"Dr Ivanova","treatmentDate":"2026-08-13"}""");

        HealthAlert alert = await(901L);
        assertThat(alert.getAnimalId()).isEqualTo(603L);
        assertThat(alert.getAnimalName()).isEqualTo("Rex");
        assertThat(alert.getSpecies()).isEqualTo(Species.DOG);
        assertThat(alert.getBreed()).isEqualTo("Husky");
        assertThat(alert.getUrgency()).isEqualTo(Urgency.CRITICAL);
        assertThat(alert.getDescription()).isEqualTo("Suspected fracture, hind leg");
        assertThat(alert.getVetName()).isEqualTo("Dr Ivanova");
        assertThat(alert.getTreatmentDate()).isEqualTo(LocalDate.of(2026, 8, 13));
        assertThat(alert.getEventId()).isEqualTo(30L);
        assertThat(alert.getReceivedAt()).isNotNull();
    }

    @Test
    void ignoresFieldsTheShelterAddsLater() {
        publish(902, 604, 31, """
                {"medicalRecordId":902,"animalId":604,"animalName":"Bella","species":"CAT",\
                "breed":"Siamese","urgency":"URGENT","description":"Refusing food",\
                "vetName":"Dr Petrov","treatmentDate":"2026-08-13","followUpDate":"2026-09-01"}""");

        assertThat(await(902L).getDescription()).isEqualTo("Refusing food");
    }

    /**
     * A crash between publishing and committing published_at makes the relay send a batch again,
     * so the same event genuinely does arrive twice. That must leave exactly one row.
     */
    @Test
    void redeliveryOfTheSameEventChangesNothing() {
        String payload = """
                {"medicalRecordId":903,"animalId":605,"animalName":"Barsik","species":"CAT",\
                "breed":"Siberian","urgency":"URGENT","description":"Limping",\
                "vetName":"Dr Petrov","treatmentDate":"2026-08-13"}""";
        publish(903, 605, 32, payload);
        await(903L);

        publish(903, 605, 32, payload);

        sleep(2000);
        assertThat(healthAlertRepository.findAll())
                .filteredOn(a -> a.getMedicalRecordId().equals(903L))
                .hasSize(1);
        assertThat(await(903L).getEventId()).isEqualTo(32L);
    }

    /**
     * Unlike the animal read model, a stored alert is never rewritten: it records what the vet
     * wrote, and the shelter emits it once. DO NOTHING means the first arrival wins outright —
     * pinned here so nobody quietly turns it into a DO UPDATE.
     * <p>
     * The shelter cannot currently produce this: one medical record yields one event.
     */
    @Test
    void aSecondEventForTheSameRecordCannotRewriteIt() {
        publish(904, 606, 33, """
                {"medicalRecordId":904,"animalId":606,"animalName":"Mira","species":"DOG",\
                "breed":"Collie","urgency":"CRITICAL","description":"Original diagnosis",\
                "vetName":"Dr Ivanova","treatmentDate":"2026-08-13"}""");
        await(904L);

        publish(904, 606, 34, """
                {"medicalRecordId":904,"animalId":606,"animalName":"OVERWRITTEN","species":"DOG",\
                "breed":"OVERWRITTEN","urgency":"ROUTINE","description":"OVERWRITTEN",\
                "vetName":"OVERWRITTEN","treatmentDate":"2020-01-01"}""");

        sleep(2000);
        HealthAlert alert = await(904L);
        assertThat(alert.getDescription()).isEqualTo("Original diagnosis");
        assertThat(alert.getUrgency()).isEqualTo(Urgency.CRITICAL);
        assertThat(alert.getEventId()).isEqualTo(33L);
    }

    /** Keyed by animalId, as the shelter's outbox relay keys it. */
    private void publish(long medicalRecordId, long animalId, long eventId, String payload) {
        ProducerRecord<String, Object> record =
                new ProducerRecord<>(Topics.HEALTH_ALERT, String.valueOf(animalId), payload);
        record.headers().add("event-id", String.valueOf(eventId).getBytes(UTF_8));
        kafkaTemplate.send(record);
    }

    private HealthAlert await(Long medicalRecordId) {
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        while (System.nanoTime() < deadline) {
            Optional<HealthAlert> found = healthAlertRepository.findById(medicalRecordId);
            if (found.isPresent()) {
                return found.get();
            }
            sleep(200);
        }
        throw new AssertionError("Health alert " + medicalRecordId + " never reached the read model");
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
