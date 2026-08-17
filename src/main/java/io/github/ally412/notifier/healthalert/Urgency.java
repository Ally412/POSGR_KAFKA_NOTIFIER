package io.github.ally412.notifier.healthalert;

public enum Urgency {
    // The shelter only publishes an alert when urgency is not ROUTINE, but the value is
    // declared anyway: the filter lives in the producer, and a change there must not
    // arrive here as an unreadable message.
    ROUTINE,
    URGENT,
    CRITICAL
}
