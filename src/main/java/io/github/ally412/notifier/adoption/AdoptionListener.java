package io.github.ally412.notifier.adoption;

import io.github.ally412.notifier.animal.AnimalRepository;
import io.github.ally412.notifier.messaging.Topics;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@Slf4j
public class AdoptionListener {

    private final AdoptionRepository adoptionRepository;
    private final AnimalRepository animalRepository;

    public AdoptionListener(AdoptionRepository adoptionRepository, AnimalRepository animalRepository) {
        this.adoptionRepository = adoptionRepository;
        this.animalRepository = animalRepository;
    }

    /**
     * One event, two read models: the adoption is a new fact, and the animal's status is state
     * the notifier already holds. Both writes share this transaction, so the notifier cannot end
     * up having recorded the adoption while still calling the animal available.
     */
    @KafkaListener(topics = Topics.ADOPTION_COMPLETED)
    @Transactional
    public void onAdoptionCompleted(AdoptionCompleted adoption, @Header("event-id") String eventId) {
        long version = Long.parseLong(eventId);

        int stored = adoptionRepository.apply(
                adoption.animalId(),
                adoption.animalName(),
                adoption.adopterId(),
                adoption.adopterName(),
                adoption.date(),
                version);

        int statusChanged = animalRepository.markAdopted(adoption.animalId(), version);

        if (stored == 0) {
            log.debug("Ignored duplicate adoption event {} for animal {}", eventId, adoption.animalId());
        } else {
            log.info("Animal {} ({}) adopted by {} on {}",
                    adoption.animalId(), adoption.animalName(), adoption.adopterName(), adoption.date());
        }

        // A fresh adoption whose animal did not move is the one combination that should never
        // happen: it means the animal is missing here, or is already at a later version.
        if (stored == 1 && statusChanged == 0) {
            log.warn("Adoption {} stored but animal {} was not marked adopted — unknown here, "
                    + "or already at a version beyond {}", eventId, adoption.animalId(), version);
        }
    }
}
