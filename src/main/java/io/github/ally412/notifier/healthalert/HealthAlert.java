package io.github.ally412.notifier.healthalert;

import io.github.ally412.notifier.animal.Species;

import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import lombok.Getter;
import lombok.Setter;

import java.time.Instant;
import java.time.LocalDate;

@Entity
@Getter
@Setter
public class HealthAlert {
    // the shelter's medical_record id — no @GeneratedValue, this table mirrors someone else's keys
    @Id
    private Long medicalRecordId;
    private Long animalId;
    private String animalName;
    @Enumerated(EnumType.STRING)
    private Species species;
    private String breed;
    @Enumerated(EnumType.STRING)
    private Urgency urgency;
    private String description;
    private String vetName;
    private LocalDate treatmentDate;
    private Long eventId;
    private Instant receivedAt;
    // null until the digest has told someone; this column is what makes the table a work queue
    private Instant notifiedAt;
}
