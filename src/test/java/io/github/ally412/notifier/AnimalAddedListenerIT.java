package io.github.ally412.notifier;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;

import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

/**
 * Feeds the listener the exact JSON the shelter app publishes — hand-written here on purpose,
 * because the two apps share no code. If the shelter's wire format drifts, this test is what
 * notices.
 * <p>
 * The notifier normally has no producer at all, so the test supplies String serializers to
 * write raw text onto the topic.
 */
@SpringBootTest(properties = {
        "spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer",
        "spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.StringSerializer"
})
@Testcontainers
class AnimalAddedListenerIT {

    @Container
    @ServiceConnection
    static ConfluentKafkaContainer kafka = new ConfluentKafkaContainer("confluentinc/cp-kafka:7.8.0");

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @MockitoSpyBean
    AnimalAddedListener listener;

    @Test
    void buildsItsOwnRecordFromTheShelterJson() {
        kafkaTemplate.send(Topics.ANIMAL_ADDED, "503",
                """
                {"animalId":503,"name":"Rex","species":"DOG","breed":"Husky"}""");

        // No __TypeId__ header on the message: spring.json.value.default.type is what
        // tells the deserializer to build OUR AnimalAdded, not the producer's class.
        verify(listener, timeout(15_000))
                .onAnimalAdded(new AnimalAdded(503L, "Rex", Species.DOG, "Husky"));
    }

    @Test
    void ignoresFieldsTheShelterAddsLater() {
        kafkaTemplate.send(Topics.ANIMAL_ADDED, "504",
                """
                {"animalId":504,"name":"Bella","species":"CAT","breed":"Siamese","intakeDate":"2026-08-03"}""");

        // Forward compatibility: the producer can add fields without redeploying us.
        // (Contrast the species enum, where a NEW VALUE would break us — a deliberate trade-off.)
        verify(listener, timeout(15_000))
                .onAnimalAdded(new AnimalAdded(504L, "Bella", Species.CAT, "Siamese"));
    }
}
