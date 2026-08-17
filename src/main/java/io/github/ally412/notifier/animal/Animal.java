package io.github.ally412.notifier.animal;

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
public class Animal {
    // the shelter's id — no @GeneratedValue, this table mirrors someone else's keys
    @Id
    private Long animalId;
    private String name;
    @Enumerated(EnumType.STRING)
    private Species species;
    private String breed;
    @Enumerated(EnumType.STRING)
    private Status status;
    private LocalDate intakeDate;
    private Long version;
    private Instant updatedAt;
}
