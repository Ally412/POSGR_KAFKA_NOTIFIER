# shelter-notifier

Consumes the shelter's events and keeps its own copy of what it hears about, in its
own database. It shares no code with the shelter — only a wire format — so every
disagreement between the two surfaces here, at deserialization, never at publish time.

For the producer side and the whole path from a domain write to this app, see
`KafkaPipeline.md` in the shelter repo. This file covers running the notifier and
the decisions that are its own.

Last updated: 2026-08-17, `main` = `113a457`.

## Running it

The broker lives in the **shelter** repo's compose file and is shared. Everything
else is here:

```bash
docker compose up -d          # postgres on 5433, mailpit on 1025/8025
./gradlew bootRun
```

There is no web server — `spring.main.web-application-type=none`. The app is a
Kafka consumer with a timer; the only way to see it working is the log, the
database, and the digests at <http://localhost:8025>.

| Variable | Default | |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | the shelter's broker |
| `DB_HOST` / `DB_PORT` | `localhost` / `5433` | 5432 is the shelter's database |
| `DB_NAME` / `DB_USER` / `DB_PASSWORD` | `notifier` | |
| `MAIL_HOST` / `MAIL_PORT` | `localhost` / `1025` | Mailpit locally, a relay in production |
| `DIGEST_RECIPIENT` | `vet@shelter.local` | who gets the alert digest |
| `KAFKA_REPLICAS` | `1` | replication factor for the dead letter topics |

Tests need Docker — every one of them runs against a real Postgres, and most
against a real broker, through Testcontainers:

```bash
./gradlew test
```

## The three read models

| Table | Fed by | Key | On conflict |
|---|---|---|---|
| `animal` | `shelter.animal.added` | shelter's `animal_id` | `DO UPDATE ... WHERE version <` |
| `health_alert` | `shelter.animal.health-alert` | shelter's `medical_record_id` | `DO NOTHING` |
| `adoption` | `shelter.adoption.completed` | shelter's `animal_id` | `DO NOTHING` |

Every key is the shelter's, never one this app mints — these tables mirror someone
else's identifiers, which is why no entity carries `@GeneratedValue`.

### Why one of them guards on a version and two do not

`version` is the shelter's **outbox row id**, carried in the `event-id` header. It
is not a JPA optimistic-lock version, and putting `@Version` on that field would
break the scheme — Hibernate would take the column over and increment it on flush.

Outbox ids come from a Postgres identity sequence, so for a given animal a higher
id always means a newer event. That makes `WHERE animal.version < excluded.version`
a total ordering rule: a redelivery is a no-op, and a late event that overtook a
newer one is rejected rather than applied. `apply()` returns 0 in both cases, which
the listener treats as success — offsets still commit.

The other two tables need none of this. The shelter emits a health alert only when
a medical record is *created*, and refuses a second adoption for an animal
outright, so those rows are written once and have no later state to lose to. There
the primary key alone provides idempotency and `DO NOTHING` is the whole story.

`AdoptionCompleted` is the interesting case: it is the only event that changes state
an earlier event already wrote (`animal.status` → `ADOPTED`). It carries just the
animal's id and name, so it cannot rebuild a row through `apply()` —
`AnimalRepository.markAdopted()` moves the one column under the same version rule.
Without that guard, an ordinary relay re-send would set an adopted animal back to
`AVAILABLE`.

## Reading the topic

One consumer factory serves three payload types. The value is deserialized as
plain text and a `StringJacksonJsonMessageConverter` picks the target type from
each `@KafkaListener` signature. A factory-wide `spring.json.value.default.type`
cannot do this: a second topic would decode into the first topic's class with every
field null and no error raised.

That is also why there is no `ErrorHandlingDeserializer`. It exists to move a
poll-loop failure — which has no error handler, no record, and no way to advance
the offset — into ordinary territory. With a `String` delegate the poll loop cannot
throw at all, and the parse it used to guard now happens during listener
invocation, where a `ConversionException` is already routed to the dead letter
topic. The raw payload survives on the record too, so failures log themselves.

Unknown **fields** are ignored, so the shelter can add to a payload without
redeploying this app. Unknown **enum values** are not: a new `Species`, `Status`
or `Urgency` fails conversion and lands in the DLT. That is a deliberate trade,
not an oversight — see *Known limitations*.

Dead letter routing derives the destination from the source topic
(`record.topic() + ".DLT"`) and preserves the partition. Each one needs its own
`NewTopic` bean; auto-creation gives a dead letter topic one partition, and
same-partition routing then has nowhere to put a failure from partition 1 or 2.

## The digest

`AlertDigestJob` mails whatever health alerts nobody has been told about yet, then
marks them. `health_alert.notified_at` is what makes the table its own work queue —
no second outbox, and nothing can be lost between being stored and being sent.

The send deliberately does not live in the listener. Mailing as records are consumed
would put an external side effect inside the transaction that commits the offset: a
mail failure would redeliver the record, and a rollback after a successful send
would mail twice with nothing to tell the difference.

Within the job the order is send, then mark, in one transaction. If the send throws,
nothing is marked and the same alerts retry next tick. The window left is a crash
between a successful send and the commit, which resends — for a health alert a
duplicate mail is the better failure than a missed one.

A transaction-scoped advisory lock (`pg_try_advisory_xact_lock`) keeps two
notifiers from mailing the same alerts, the same guard the shelter's outbox relay
uses and for the same reason: taken outside a transaction it would be released
before any work happened.

## Known limitations

- **The `animal` read model drifts.** `updateAnimal`, `updateStatus`,
  `updateStatusSocializingToAvailable` and `deleteAnimal` change the shelter's
  database and emit nothing, so anything but a creation or an adoption goes
  unnoticed here. Accepted, not planned — this is a study project.
- **A delete would resurrect.** If deletions ever *were* published, the version
  guard could not stop them: its `WHERE` gates only the `DO UPDATE` branch, so a
  late update arriving after a delete finds no row, hits no conflict, skips the
  check, and re-inserts. A `deleted boolean` would make deletion an ordinary
  guarded update and close it.
- **`Species`, `Status` and `Urgency` are hand-copied** from the shelter. A new
  constant there breaks this app until redeployed. `@JsonEnumDefaultValue` on an
  `UNKNOWN` member would soften it.
- **Nothing reads `animal` or `adoption`.** Only `health_alert` has anything
  downstream.
- **Test cost and flakiness.** Four `@SpringBootTest` classes each start their own
  containers (~1m51s). Three tests use a fixed `sleep(2000)` to wait for something
  *not* to happen, and `AlertDigestJobIT` binds GreenMail to a fixed port 3025,
  which collides if two builds run at once.
