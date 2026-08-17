package io.github.ally412.notifier.healthalert;

import com.icegreen.greenmail.junit5.GreenMailExtension;
import com.icegreen.greenmail.util.ServerSetupTest;
import io.github.ally412.notifier.animal.Species;

import jakarta.mail.internet.MimeMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.time.Instant;
import java.time.LocalDate;

import static com.icegreen.greenmail.util.GreenMailUtil.getBody;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The digest against a real SMTP server and a real database. Kafka is not needed here — the
 * listeners are switched off, because what this covers starts once a row is already stored.
 */
@SpringBootTest(properties = {
        "spring.mail.host=localhost",
        "spring.mail.port=3025",
        "spring.kafka.listener.auto-startup=false",
        // Keep the scheduler out of it: every send in these tests is an explicit call.
        "notifier.digest.initial-delay=3600000",
        "notifier.digest.interval=3600000"
})
@Testcontainers
class AlertDigestJobIT {

    @Container
    @ServiceConnection
    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17");

    @RegisterExtension
    static GreenMailExtension greenMail = new GreenMailExtension(ServerSetupTest.SMTP)
            .withPerMethodLifecycle(true);

    @Autowired
    AlertDigestJob alertDigestJob;

    @Autowired
    HealthAlertRepository healthAlertRepository;

    @BeforeEach
    void clearAlerts() {
        healthAlertRepository.deleteAll();
    }

    @Test
    void mailsEveryUnnotifiedAlertInOneMessage() throws Exception {
        store(801L, 601L, "Rex", Urgency.CRITICAL, "Suspected fracture, hind leg");
        store(802L, 602L, "Bella", Urgency.URGENT, "Refusing food");

        alertDigestJob.sendDigest();

        MimeMessage[] received = greenMail.getReceivedMessages();
        assertThat(received).hasSize(1);
        assertThat(received[0].getSubject()).isEqualTo("2 health alert(s) need attention");

        String body = getBody(received[0]);
        assertThat(body)
                .contains("[CRITICAL]", "Rex", "Suspected fracture, hind leg")
                .contains("[URGENT]", "Bella", "Refusing food");
    }

    @Test
    void marksWhatItSentSoTheNextRunSendsNothing() {
        store(803L, 603L, "Barsik", Urgency.URGENT, "Limping");

        alertDigestJob.sendDigest();
        assertThat(greenMail.getReceivedMessages()).hasSize(1);
        assertThat(healthAlertRepository.findById(803L).orElseThrow().getNotifiedAt()).isNotNull();

        alertDigestJob.sendDigest();

        // Still the one message: an alert is reported once, however often the job runs.
        assertThat(greenMail.getReceivedMessages()).hasSize(1);
    }

    @Test
    void sendsNothingWhenNobodyNeedsTelling() {
        alertDigestJob.sendDigest();

        assertThat(greenMail.getReceivedMessages()).isEmpty();
    }

    private void store(Long medicalRecordId, Long animalId, String animalName,
                       Urgency urgency, String description) {
        HealthAlert alert = new HealthAlert();
        alert.setMedicalRecordId(medicalRecordId);
        alert.setAnimalId(animalId);
        alert.setAnimalName(animalName);
        alert.setSpecies(Species.DOG);
        alert.setBreed("Husky");
        alert.setUrgency(urgency);
        alert.setDescription(description);
        alert.setVetName("Dr Ivanova");
        alert.setTreatmentDate(LocalDate.of(2026, 8, 17));
        alert.setEventId(medicalRecordId);
        // received_at is NOT NULL with a database default, but a JPA insert names every column,
        // so the default never applies and the value has to be set here.
        alert.setReceivedAt(Instant.now());
        healthAlertRepository.save(alert);
    }
}
