# shelter-notifier

Kafka consumer for the shelter's events. See `README.md` for the design and
`../POSGR_KAFKA/KafkaPipeline.md` for the whole producer-to-consumer path — don't
restate either here.

## The producer is a separate repo

`/home/av/POSGR_KAFKA`. The two apps share no code, only a wire format. **Read it
rather than guessing the contract** — the event classes, `OutboxRelay` and
`AnimalService` answer most questions about what arrives here and why.

## Build cadence

A run costs ~2 minutes: every test class starts its own Kafka and Postgres. So do not
run Gradle on reflex.

- While building a feature: **don't run Gradle at all.**
- When told the feature is done and to write tests: write *all* of them, then run
  `./gradlew test` **once**.
- `./gradlew build` only immediately before a commit.

Tests need Docker (Testcontainers).

**`UP-TO-DATE` is not a pass.** Gradle replays a cached result when inputs are
unchanged. That's legitimate, but never report it as evidence tests ran — use
`--rerun-tasks` when proof is needed. This has already produced one fake green.

## Conventions

Packages by domain — `animal`, `healthalert`, `adoption`, `messaging` — mirroring the
shelter's layout so the same concept sits at the same path in both repos.

Read models are keyed by **the shelter's** ids, never generated here. No entity carries
`@GeneratedValue`.

## The rule that is easy to break

`Animal.version` is the shelter's **outbox row id**, carried in the `event-id` header.
It is *not* a JPA optimistic-lock version. Putting `@Version` on that field would hand
the column to Hibernate, which would increment it on flush and silently break the
ordering guarantee the whole read model rests on.

## Scope

Study project. **No new functionality** — hardening, tests and docs only.

The `animal` read model drifts: the shelter's `updateAnimal`, `updateStatus`,
`updateStatusSocializingToAvailable` and `deleteAnimal` emit no events. That is a known
and accepted limitation, not a to-do.
