package io.github.ally412.notifier;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@Slf4j
public class AnimalAddedListener {

    private final AnimalRepository animalRepository;

    public AnimalAddedListener(AnimalRepository animalRepository) {
        this.animalRepository = animalRepository;
    }

    @KafkaListener(topics = Topics.ANIMAL_ADDED)
    @Transactional
    public void onAnimalAdded(AnimalAdded animal, @Header("event-id") String eventId) {
        int applied = animalRepository.apply(
                animal.animalId(),
                animal.name(),
                animal.species().name(),
                animal.breed(),
                animal.status().name(),
                animal.intakeDate(),
                Long.parseLong(eventId));

        if (applied == 0) {
            log.debug("Ignored stale or duplicate event {} for animal {}", eventId, animal.animalId());
            return;
        }
        log.info("Applied animal {}: {}, {}, {}", animal.animalId(), animal.name(), animal.species(), animal.breed());
    }
}
