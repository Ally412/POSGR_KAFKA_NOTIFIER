-- Adoptions the notifier has heard about. Fed only by events, like the other two tables.
-- The shelter's adoption row is @MapsId onto the animal, so an animal has at most one
-- adoption and the animal's id is the natural key here too.
CREATE TABLE adoption (
                        animal_id    bigint       PRIMARY KEY,
                        -- animal and adopter as the event carried them: a snapshot, not a
                        -- lookup, since the notifier stores no adopters of its own
                        animal_name  varchar(255) NOT NULL,
                        adopter_id   bigint       NOT NULL,
                        adopter_name varchar(255) NOT NULL,
                        date         date         NOT NULL,
                        -- outbox row id, for tracing a row back to its event. No version
                        -- comparison: the shelter refuses a second adoption for an animal, so
                        -- this row is written once and never revised.
                        event_id     bigint       NOT NULL,
                        received_at  timestamptz  NOT NULL DEFAULT now()
);
