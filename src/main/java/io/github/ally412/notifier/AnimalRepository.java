package io.github.ally412.notifier;

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
}
