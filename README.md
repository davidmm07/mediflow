# MediFlow: Telemedicine platform on Go microservices

[![CI](https://github.com/davidmm07/mediflow/actions/workflows/ci.yml/badge.svg)](https://github.com/davidmm07/mediflow/actions/workflows/ci.yml)
[![Go](https://img.shields.io/badge/Go-1.25-00ADD8?logo=go&logoColor=white)](https://go.dev)
[![Contract tests](https://img.shields.io/badge/contract%20tests-Pact-brightgreen)](https://docs.pact.io)

A working, distributed telehealth booking platform: five Go services, OAuth2/OIDC
identity through Keycloak, per-service MongoDB, Kafka for asynchronous
integration, and **consumer-driven contract testing with Pact** covering both
the synchronous and the event-driven boundaries.

The interesting part is not the CRUD. It is what happens between the services:
a booking that spans an HTTP call and two databases, a compensating action when
it half-fails, and a set of contracts that make those boundaries fail loudly in
CI instead of quietly in production.

---

## Why telemedicine

Remote consultation platforms are a domain where the distributed-systems
problems are real rather than decorative:

- **Contention is inherent.** Two patients clicking the same 09:00 slot is the
  normal case, not an edge case. It forces a real answer on atomicity across a
  service boundary.
- **Consistency requirements differ per flow.** Reserving a slot must be
  immediate and correct. Sending the confirmation notification must not be
  allowed to fail the booking. That difference is what justifies having both a
  synchronous call and an event bus, rather than picking one because it is
  fashionable.
- **Identity is not an afterthought.** Patient, doctor and back-office are
  genuinely different authorities over the same records.

## The business flow

```
Patient registers ──▶ auth-service ──▶ Keycloak (identity created)
                            │
                            └── auth.user.registered ──┬──▶ patient-service (provisions profile)
                                                       └──▶ notification-service (welcome message)

Patient books ──▶ appointment-service ──HTTP──▶ doctor-service (reserve slot, 409 if taken)
                            │
                            ├── writes the appointment
                            └── appointments.created ──▶ notification-service (confirmation)

Patient cancels ─▶ appointment-service ──HTTP──▶ doctor-service (release slot)
                            └── appointments.cancelled ──▶ notification-service
```

---

## Architecture

```
                         ┌──────────────────────────┐
     client ───────────▶ │  gateway  :8080          │  validates the JWT once,
                         │  (reverse proxy + authn) │  forwards it downstream
                         └────────────┬─────────────┘
                                      │
   ┌────────────────┬─────────────────┼───────────────────┬────────────────────┐
   ▼                ▼                 ▼                   ▼                    ▼
┌────────────┐ ┌────────────┐ ┌────────────────┐ ┌──────────────────┐ ┌──────────────────┐
│ auth       │ │ patient    │ │ doctor         │ │ appointment      │ │ notification     │
│ service    │ │ service    │ │ service        │ │ service          │ │ service          │
├────────────┤ ├────────────┤ ├────────────────┤ ├──────────────────┤ ├──────────────────┤
│ Keycloak   │ │ MongoDB    │ │ MongoDB        │ │ MongoDB          │ │ MongoDB          │
│ Admin API  │ │ (patients) │ │ (doctors)      │ │ (appointments)   │ │ (notifications)  │
└─────┬──────┘ └─────▲──────┘ └───────▲────────┘ └────┬────────┬────┘ └────────▲─────────┘
      │              │                │               │        │               │
      │              │                └───HTTP────────┘        │               │
      │              │              (reserve / release slot)   │               │
      │              │                                         │               │
      └──────────────┴─────────────── Kafka ───────────────────┴───────────────┘
                     auth.user.registered · appointments.created · appointments.cancelled
```

Every service verifies the Keycloak JWT itself against the realm's JWKS. The
gateway is a first filter and a convenience, never the only gate. A service
reached directly is still protected.

### Services

| Service | Responsibility | Storage | Integration role |
|---|---|---|---|
| `gateway` | Single public entry point, edge JWT validation, path routing | none | none |
| `auth-service` | Self-registration via the Keycloak Admin API, token introspection | Keycloak | Kafka **producer** |
| `doctor-service` | Practitioner directory, availability, slot reservation | MongoDB | Pact **provider** (HTTP) |
| `patient-service` | Patient profiles, provisioned reactively from identity events | MongoDB | Kafka **consumer** |
| `appointment-service` | Booking and cancellation, the booking saga | MongoDB | Pact **consumer** (HTTP) + Kafka **producer** |
| `notification-service` | Renders events into a user-facing inbox | MongoDB | Kafka **consumer**, Pact **consumer** (messages) |

---

## The differentiator: contract testing with Pact

Integration tests that boot the whole stack are slow and flaky. Worse, they
tell you a pair of services worked *together at one moment*, not that either one
is safe to deploy alone. Contract tests invert that: each side is verified
independently against a shared, versioned agreement.

MediFlow covers **both** kinds of boundary. The message half is the one usually
skipped, and it is the one that matters most on an event bus: a broker will
happily deliver a payload whose shape nobody agreed on.

### 1. HTTP contract: `appointment-service` → `doctor-service`

**Consumer side** ([`client_pact_test.go`](services/appointment-service/internal/doctorclient/client_pact_test.go))
does not describe requests in prose. It runs the *real* `doctorclient` code
against Pact's mock provider, so the generated contract cannot drift from the
client:

```go
ExecuteTest(t, func(config consumer.MockServerConfig) error {
    _, err := clientFor(config).ReserveSlot(ctx, bearerToken, doctorID, takenSlotID, appointmentID)
    require.ErrorIs(t, err, doctorclient.ErrSlotTaken)   // the 409 path, pinned
    return nil
})
```

**Provider side** ([`provider_pact_test.go`](services/doctor-service/internal/api/provider_pact_test.go))
replays every recorded interaction against the real chi router and real
handlers. Provider states seed an in-memory store, so verification needs no
MongoDB and each interaction gets exactly the fixtures it declared.

Interactions covered: fetching a profile, the 404, listing available slots,
reserving, **losing the reservation race (409)**, and releasing after a
cancellation.

### 2. Message contracts: Kafka events

`notification-service` declares what it reads from each event
([`events_pact_test.go`](services/notification-service/internal/events/events_pact_test.go)).
Pact reifies an example event and feeds it through the production
`Notifier.Handle`; the assertions then check a correct notification came out.

The producers verify those contracts against code that genuinely publishes:
[`appointment-service`](services/appointment-service/internal/booking/message_provider_pact_test.go)
runs a real booking through `booking.Service` and returns whatever it emitted;
[`auth-service`](services/auth-service/internal/api/message_provider_pact_test.go)
drives its real registration handler.

### Does it actually catch anything?

Renaming one JSON tag in the producer (`doctor_name` to `physician_name`)
fails verification with an exact diff:

```
      has a matching body (FAILED)
Failures:
    $ -> Actual map is missing the following keys: doctor_name
    -  "doctor_name": "Gregory House",
    +  "physician_name": "Gregory House",
There were 2 pact failures
```

Unit tests on both services stay green through that change. The contract is the
only thing that notices.

### Generated contracts

The [`pacts/`](pacts/) directory is committed so the artifacts are readable
without running anything:

| Contract | Kind | Interactions |
|---|---|---|
| `appointment-service-doctor-service.json` | HTTP | 6 |
| `notification-service-appointment-service.json` | Message | 2 |
| `notification-service-auth-service.json` | Message | 1 |

---

## Design decisions worth defending

**Ordering in the booking saga is deliberate, and different in each direction.**
`Book` reserves the slot in doctor-service *before* writing locally, because the
reservation is the contended resource. Reserving first means a losing racer
never creates an orphan appointment. If the local write then fails, the
reservation is released as a compensating action
([`booking.go`](services/appointment-service/internal/booking/booking.go)).
`Cancel` inverts it: the local write goes first, because it is the record the
patient sees, and a failed release costs only a recoverable slot rather than an
appointment the patient believes is cancelled but isn't.

**Event publishing never fails a durable operation.** Once Keycloak has created
the user or Mongo has stored the appointment, a broker outage is logged and
swallowed. The patient has their appointment; the reminder is late. Rolling back
would be strictly worse.

**Reservation is atomic at the database, not in application code.** The Mongo
filter includes `reserved: false`, so `FindOneAndUpdate` makes the race
unwinnable by two callers; a unique index on `slot_id` in appointment-service is
the second line of defence.

**Patients address `/patients/me`, never `/patients/{id}`.** Identity comes from
the token, so there is no id to guess. Where an id is unavoidable
(`GET /appointments/{id}`), a non-owner gets **404 rather than 403**, because a
403 would confirm the appointment exists.

**Consumers declare only the fields they read.** `notification-service`'s event
structs are subsets, so a producer adding a field is not a breaking change.
That is a property the contract encodes, not a convention people remember.

**Database per service, no shared schema.** `patient-service` is provisioned by
an event rather than by `auth-service` writing into its database. That is what
keeps the two independently deployable.

---

## Running it

Requirements: Docker with Compose. For the test suites: Go 1.25+.

```bash
make up
```

Brings up Keycloak (realm pre-imported), Redpanda (Kafka API), four MongoDB
instances, a Pact Broker, and the six Go services. First boot takes a couple of
minutes while Keycloak imports the realm.

| Endpoint | URL | Credentials |
|---|---|---|
| API gateway | http://localhost:8080 | none |
| Keycloak admin | http://localhost:8081 | `admin` / `admin` |
| Pact Broker | http://localhost:9292 | `pact` / `pact` |

Seeded realm users: `dr.house` / `doctor123` (doctor), `ana.paciente` /
`patient123` (patient), `admin.ops` / `admin123` (admin).

> **Every credential in this repository is a throwaway local-development
> value**: the Keycloak admin password, the `mediflow-auth-service` client
> secret, the Pact Broker login and the seeded users' passwords. They exist so
> `make up` gives you a working stack in one command, and they are committed
> deliberately for that reason. Nothing here is, or has ever been, a real
> secret.
>
> The realm export is likewise development-shaped: `sslRequired: none`, a
> public client with the direct-access grant enabled so the smoke test can
> fetch tokens, and wildcard web origins. Deploying it unchanged would be a
> security hole: real deployments need rotated secrets, TLS enforced,
> Authorization Code + PKCE instead of the password grant, and the seeded
> users deleted.

### End-to-end smoke test

```bash
make smoke
```

Walks the full distributed flow: register → obtain a real Keycloak token →
publish a doctor profile and a slot → book → assert the slot disappears →
**poll until the Kafka-driven notification arrives** → cancel → assert the slot
is released. The notification step is the one that proves the async path works;
no HTTP response tells you the event was delivered.

### Talking to it by hand

```bash
TOKEN=$(curl -s -X POST http://localhost:8081/realms/mediflow/protocol/openid-connect/token \
  -d client_id=mediflow-gateway -d grant_type=password \
  -d username=ana.paciente -d password=patient123 | jq -r .access_token)
```

```bash
curl -s http://localhost:8080/doctors -H "Authorization: Bearer $TOKEN" | jq
```

---

## Tests

```bash
make check
```

Vet plus unit tests across all seven modules. No native dependencies, no
containers, and fast enough to run on every save.

```bash
make pact-install   # one-off: downloads the Pact FFI library
make pact           # consumers generate contracts, providers verify them
```

The Pact suites sit behind the `pact` build tag so `make test` stays
dependency-free. Order matters: consumers run first because they *generate* the
contracts; providers then replay them.

Unit tests focus on the behaviour that is easy to get wrong and invisible in a
happy-path demo: the compensating release when persistence fails, cancellation
authorization, slot-overlap symmetry, idempotent handling of a redelivered Kafka
event, and malformed payloads being dropped rather than retried forever.

---

## CI

[`.github/workflows/ci.yml`](.github/workflows/ci.yml) runs four stages:

1. **unit**: vet and unit-test every module.
2. **contracts**: install the Pact FFI, generate the consumer contracts, then
   verify every provider against them. This is the gate that stops a service
   shipping a change its collaborators cannot handle.
3. **publish-pacts**: trunk only, and only after verification is green; the
   broker should never hold a contract that failed.
4. **images**: build all six container images, catching Dockerfile drift.

`make can-i-deploy` queries the broker for whether a given version is safe to
release against everything already deployed.

---

## Layout

```
mediflow/
├── common/                     shared: JWKS auth middleware, Kafka envelope,
│                               Mongo helper, HTTP helpers, graceful shutdown
├── gateway/                    public entry point
├── services/
│   ├── auth-service/           registration + Keycloak Admin API
│   ├── doctor-service/         directory, availability, slot reservation
│   ├── patient-service/        profiles, event-driven provisioning
│   ├── appointment-service/    booking saga
│   └── notification-service/   event-driven inbox
├── pacts/                      generated contracts
├── deploy/
│   ├── Dockerfile              one multi-stage build, SERVICE arg selects module
│   └── keycloak/               realm export: roles, clients, seeded users
├── scripts/smoke-test.sh
├── docker-compose.yml
└── Makefile
```

A Go workspace (`go.work`) with one module per service: each has its own
dependency graph and could be extracted into its own repository without
restructuring, while local development still builds everything at once.

## Stack

Go 1.25 · MongoDB 7 · Kafka (Redpanda) · Keycloak 24 · Pact (pact-go v2) ·
chi · zerolog · segmentio/kafka-go · lestrrat-go/jwx · Docker Compose ·
GitHub Actions
