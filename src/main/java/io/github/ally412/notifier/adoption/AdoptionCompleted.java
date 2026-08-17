package io.github.ally412.notifier.adoption;

import java.time.LocalDate;

public record AdoptionCompleted(Long animalId,
                                String animalName,
                                Long adopterId,
                                String adopterName,
                                LocalDate date) {
}
