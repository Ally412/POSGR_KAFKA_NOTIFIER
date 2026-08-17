package io.github.ally412.notifier.animal;

import java.time.LocalDate;

public record AnimalAdded(Long animalId,
                          String name,
                          Species species,
                          String breed,
                          Status status,
                          LocalDate intakeDate) {
}
