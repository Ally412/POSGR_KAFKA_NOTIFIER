-- Alerts the notifier has heard about: one row per medical record the shelter flagged as
-- worse than ROUTINE. Fed only by events, like `animal`, and authoritative for nothing.
CREATE TABLE health_alert (
                        -- the shelter's medical_record id, not one we mint. As the primary key it
                        -- makes a redelivered event collide with the row it already produced,
                        -- which is the whole of the idempotency an alert needs.
                        medical_record_id bigint       PRIMARY KEY,
                        animal_id         bigint       NOT NULL,
                        -- the animal as it was when the vet wrote the record. Deliberately a
                        -- snapshot carried by the event rather than a lookup into `animal`, so
                        -- the alert still reads correctly after a later rename, and so it can be
                        -- stored even if the animal is somehow unknown here.
                        animal_name       varchar(255) NOT NULL,
                        species           varchar(255) NOT NULL,
                        breed             varchar(255) NOT NULL,
                        urgency           varchar(255) NOT NULL,
                        description       varchar(255) NOT NULL,
                        vet_name          varchar(255) NOT NULL,
                        treatment_date    date         NOT NULL,
                        -- outbox row id, kept so a row can be traced back to the event that made
                        -- it. No version comparison as in `animal`: the shelter emits an alert
                        -- only when a medical record is created, so an alert is an immutable
                        -- fact — a duplicate is ignored, and there is no later state to lose to.
                        event_id          bigint       NOT NULL,
                        -- when the notifier learned of it, not when the vet recorded it
                        received_at       timestamptz  NOT NULL DEFAULT now()
);

-- Alerts are read per animal ("what has this one been flagged for"), and animal_id is not
-- the key.
CREATE INDEX idx_health_alert_animal ON health_alert (animal_id);
