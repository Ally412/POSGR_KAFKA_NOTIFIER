package io.github.ally412.notifier.adoption;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.Getter;
import lombok.Setter;

import java.time.Instant;
import java.time.LocalDate;

@Entity
@Getter
@Setter
public class Adoption {
    // the shelter's animal id — an animal has at most one adoption, so it keys this table too
    @Id
    private Long animalId;
    private String animalName;
    private Long adopterId;
    private String adopterName;
    private LocalDate date;
    private Long eventId;
    private Instant receivedAt;
}
