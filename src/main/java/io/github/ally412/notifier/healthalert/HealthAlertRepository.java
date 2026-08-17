package io.github.ally412.notifier.healthalert;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDate;

public interface HealthAlertRepository extends JpaRepository<HealthAlert, Long> {

    /**
     * Applies an event to the read model. Native because JPQL has no ON CONFLICT.
     * <p>
     * DO NOTHING rather than the version comparison {@code animal} uses: the shelter emits an
     * alert only when a medical record is created, so an alert is an immutable fact. There is
     * no later state a redelivery could overwrite, and the primary key is the shelter's own
     * medical_record_id, so a duplicate collides with the row it already produced.
     * <p>
     * Returns 0 when the alert was already stored.
     */
    @Modifying
    @Query(value = """
            INSERT INTO health_alert (medical_record_id, animal_id, animal_name, species, breed,
                                      urgency, description, vet_name, treatment_date, event_id,
                                      received_at)
            VALUES (:medicalRecordId, :animalId, :animalName, :species, :breed,
                    :urgency, :description, :vetName, :treatmentDate, :eventId,
                    now())
            ON CONFLICT (medical_record_id) DO NOTHING
            """, nativeQuery = true)
    int apply(@Param("medicalRecordId") Long medicalRecordId,
              @Param("animalId") Long animalId,
              @Param("animalName") String animalName,
              @Param("species") String species,
              @Param("breed") String breed,
              @Param("urgency") String urgency,
              @Param("description") String description,
              @Param("vetName") String vetName,
              @Param("treatmentDate") LocalDate treatmentDate,
              @Param("eventId") Long eventId);
}
