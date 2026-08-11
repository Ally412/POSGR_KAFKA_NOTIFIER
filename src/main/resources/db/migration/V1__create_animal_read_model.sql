-- The notifier's own copy of the animals it has heard about. Fed only by events;
-- nothing here is authoritative, and the shelter never reads it.
CREATE TABLE animal (
                        -- the shelter's id, not one we mint: this table mirrors someone else's keys
                        animal_id   bigint        PRIMARY KEY,
                        name        varchar(255)  NOT NULL,
                        species     varchar(255)  NOT NULL,
                        breed       varchar(255)  NOT NULL,
                        status      varchar(255)  NOT NULL,
                        intake_date date          NOT NULL,
                        -- outbox row id from the event-id header; an event older than the row
                        -- it would overwrite is ignored, so out-of-order delivery is harmless
                        version     bigint        NOT NULL,
                        updated_at  timestamptz   NOT NULL DEFAULT now()
);
