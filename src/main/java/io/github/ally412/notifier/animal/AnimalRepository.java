package io.github.ally412.notifier.animal;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDate;

public interface AnimalRepository extends JpaRepository<Animal, Long> {

    /**
     * Applies an event to the read model. Native because JPQL has no ON CONFLICT, and because
     * the version check has to happen inside the statement to be atomic.
     * <p>
     * Returns 0 when the event is older than the row it would overwrite — the WHERE turns a
     * stale or duplicate delivery into a no-op instead of an error.
     */
    @Modifying
    @Query(value = """
            INSERT INTO animal (animal_id, name, species, breed, status, intake_date, version, updated_at)
            VALUES (:animalId, :name, :species, :breed, :status, :intakeDate, :version, now())
            ON CONFLICT (animal_id) DO UPDATE SET
                name        = excluded.name,
                species     = excluded.species,
                breed       = excluded.breed,
                status      = excluded.status,
                intake_date = excluded.intake_date,
                version     = excluded.version,
                updated_at  = now()
            WHERE animal.version < excluded.version
            """, nativeQuery = true)
    int apply(@Param("animalId") Long animalId,
              @Param("name") String name,
              @Param("species") String species,
              @Param("breed") String breed,
              @Param("status") String status,
              @Param("intakeDate") LocalDate intakeDate,
              @Param("version") long version);

    /**
     * Moves an animal to ADOPTED. Separate from {@link #apply} because AdoptionCompleted carries
     * only the animal's id and name — it cannot rebuild a whole row, so it touches one column.
     * <p>
     * The same version rule applies, and here it finally does real work: this is the first event
     * that changes state an earlier event already wrote, so an AnimalAdded redelivered after the
     * adoption must not set the status back.
     * <p>
     * Returns 0 both when the animal is unknown here and when the event is not newer than the
     * stored row. The caller distinguishes them, because the first means the read model drifted.
     */
    @Modifying
    @Query(value = """
            UPDATE animal
               SET status     = 'ADOPTED',
                   version    = :version,
                   updated_at = now()
             WHERE animal_id = :animalId
               AND version < :version
            """, nativeQuery = true)
    int markAdopted(@Param("animalId") Long animalId, @Param("version") long version);
}
