package io.github.ally412.notifier.healthalert;

import io.github.ally412.notifier.messaging.Topics;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@Slf4j
public class HealthAlertListener {
    private final HealthAlertRepository healthAlertRepository;

    public HealthAlertListener(HealthAlertRepository healthAlertRepository) {
        this.healthAlertRepository = healthAlertRepository;
    }

    @KafkaListener(topics = Topics.HEALTH_ALERT)
    @Transactional
    public void onHealthAlert(HealthAlertAdded alert, @Header("event-id") String eventId) {
        int applied = healthAlertRepository.apply(
                alert.medicalRecordId(),
                alert.animalId(),
                alert.animalName(),
                alert.species().name(),
                alert.breed(),
                alert.urgency().name(),
                alert.description(),
                alert.vetName(),
                alert.treatmentDate(),
                Long.parseLong(eventId));

        if (applied == 0) {
            log.debug("Ignored duplicate health alert {} for medical record {}",
                    eventId, alert.medicalRecordId());
            return;
        }
        // Only non-ROUTINE records reach this topic, so everything stored here is worth seeing.
        log.warn("Health alert [{}] for animal {} ({}): {} — recorded by {} on {}",
                alert.urgency(),
                alert.animalId(),
                alert.animalName(),
                alert.description(),
                alert.vetName(),
                alert.treatmentDate());
    }
}
