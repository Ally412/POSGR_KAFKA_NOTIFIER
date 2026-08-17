package io.github.ally412.notifier.healthalert;

import io.github.ally412.notifier.animal.Species;

import java.time.LocalDate;

public record HealthAlertAdded(Long medicalRecordId,
                          Long animalId,
                          String animalName,
                          Species species,
                          String breed,
                          Urgency urgency,
                          String description,
                          String vetName,
                          LocalDate treatmentDate) {
}
