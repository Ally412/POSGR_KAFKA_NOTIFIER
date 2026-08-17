package io.github.ally412.notifier.healthalert;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

/**
 * Mails whoever cares about the alerts that have arrived since the last run.
 * <p>
 * The send does not live in the Kafka listener on purpose. A listener that mailed as it consumed
 * would put an external side effect inside the transaction that commits the offset: a mail
 * failure would redeliver the record, and a rollback after a successful send would mail twice
 * with no way to tell. Here the listener only stores, and this job — driven by the table itself —
 * decides what still needs sending.
 */
@Component
@Slf4j
public class AlertDigestJob {

    // Advisory-lock key: whoever holds it is the single digest running this tick, so two
    // notifiers cannot mail the same alerts. Distinct from the shelter's relay key, which lives
    // in a different database, but kept obviously different anyway.
    private static final long ALERT_DIGEST_LOCK = 774_100_002L;

    private final HealthAlertRepository healthAlertRepository;
    private final JavaMailSender mailSender;
    private final String from;
    private final String to;
    private final int batchSize;

    public AlertDigestJob(HealthAlertRepository healthAlertRepository,
                          JavaMailSender mailSender,
                          @Value("${notifier.digest.from}") String from,
                          @Value("${notifier.digest.to}") String to,
                          @Value("${notifier.digest.batch-size:50}") int batchSize) {
        this.healthAlertRepository = healthAlertRepository;
        this.mailSender = mailSender;
        this.from = from;
        this.to = to;
        this.batchSize = batchSize;
    }

    /**
     * Sends one mail covering everything outstanding, then marks those rows notified. The order
     * matters: if the send throws, the transaction rolls back and nothing is marked, so the same
     * alerts are retried next tick rather than being silently dropped.
     * <p>
     * The one window left is a crash between a successful send and the commit, which resends
     * next tick. For a health alert a duplicate mail is the better failure than a missed one.
     */
    @Scheduled(fixedDelayString = "${notifier.digest.interval:60000}",
               initialDelayString = "${notifier.digest.initial-delay:10000}")
    @Transactional
    public void sendDigest() {
        // The guard must be inside the transactional method: the lock is transaction-scoped, so
        // taken outside one it would be released before any work happened.
        if (!healthAlertRepository.tryClaimDigest(ALERT_DIGEST_LOCK)) {
            return;
        }

        List<HealthAlert> pending = healthAlertRepository.findUnnotified(PageRequest.of(0, batchSize));
        if (pending.isEmpty()) {
            return;
        }

        mailSender.send(compose(pending));

        Instant sentAt = Instant.now();
        pending.forEach(alert -> alert.setNotifiedAt(sentAt));
        log.info("Health alert digest sent to {} covering {} alert(s)", to, pending.size());
    }

    private SimpleMailMessage compose(List<HealthAlert> alerts) {
        StringBuilder body = new StringBuilder();
        for (HealthAlert alert : alerts) {
            body.append("[%s] %s (#%d, %s %s)%n".formatted(
                            alert.getUrgency(),
                            alert.getAnimalName(),
                            alert.getAnimalId(),
                            alert.getSpecies(),
                            alert.getBreed()))
                    .append("  %s%n".formatted(alert.getDescription()))
                    .append("  recorded by %s on %s%n%n".formatted(
                            alert.getVetName(), alert.getTreatmentDate()));
        }

        SimpleMailMessage message = new SimpleMailMessage();
        message.setFrom(from);
        message.setTo(to);
        message.setSubject("%d health alert(s) need attention".formatted(alerts.size()));
        message.setText(body.toString());
        return message;
    }
}
