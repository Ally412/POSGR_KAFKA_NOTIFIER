package io.github.ally412.notifier.messaging;

public final class Topics {
    public static final String ANIMAL_ADDED = "shelter.animal.added";
    public static final String ADOPTION_COMPLETED = "shelter.adoption.completed";
    public static final String HEALTH_ALERT = "shelter.animal.health-alert";
    /**
     * Every dead letter topic is its source topic plus this suffix. The error handler routes by
     * the same rule, so a failure can only ever land beside the topic it came from — and each
     * one below still needs a NewTopic bean, or the broker auto-creates it with one partition
     * and same-partition routing breaks.
     */
    public static final String DLT_SUFFIX = ".DLT";
    public static final String ANIMAL_ADDED_DLT = ANIMAL_ADDED + DLT_SUFFIX;
    public static final String HEALTH_ALERT_DLT = HEALTH_ALERT + DLT_SUFFIX;
    private Topics() {}
}
