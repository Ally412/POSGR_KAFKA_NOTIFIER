package io.github.ally412.notifier.adoption;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDate;

public interface AdoptionRepository extends JpaRepository<Adoption, Long> {

    /**
     * Records an adoption. DO NOTHING rather than a version comparison, for the same reason as
     * {@code health_alert}: the shelter throws AnimalAlreadyAdoptedException on a second
     * adoption, so this row is written once and has no later state to lose to.
     * <p>
     * Returns 0 when the adoption was already stored.
     */
    @Modifying
    @Query(value = """
            INSERT INTO adoption (animal_id, animal_name, adopter_id, adopter_name, date,
                                  event_id, received_at)
            VALUES (:animalId, :animalName, :adopterId, :adopterName, :date,
                    :eventId, now())
            ON CONFLICT (animal_id) DO NOTHING
            """, nativeQuery = true)
    int apply(@Param("animalId") Long animalId,
              @Param("animalName") String animalName,
              @Param("adopterId") Long adopterId,
              @Param("adopterName") String adopterName,
              @Param("date") LocalDate date,
              @Param("eventId") Long eventId);
}
