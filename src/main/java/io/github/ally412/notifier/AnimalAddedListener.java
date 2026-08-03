package io.github.ally412.notifier;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class AnimalAddedListener {
    @KafkaListener(topics = Topics.ANIMAL_ADDED)
    public void onAnimalAdded(AnimalAdded animal) {
        log.info("New animal: {}, {}, {}", animal.name(), animal.species(), animal.breed());
    }
}
