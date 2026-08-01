# Contrail

A streaming data pipeline that reconstructs **flights** from raw aircraft position
reports published by the [OpenSky Network](https://opensky-network.org/).

The API publishes *aircraft*, not flights: a stream of transponder positions keyed
by airframe, with no notion of where one journey ends and the next begins. Turning
that into flights, under a hard daily quota and with duplicate and badly stale
observations mixed in, is the problem this pipeline solves.

```
OpenSky REST API
      │  Go ingester: OAuth2, credit budgeter, cost-aware region scheduler
      ▼
  Redpanda  (Kafka API, keyed by ICAO24 so each aircraft stays ordered)
      │  Python sink: schema validation, at-least-once delivery
      ├──────────────► MinIO   Parquet, partitioned by observation hour  (bronze)
      ▼
 ClickHouse  ReplacingMergeTree, deduplicating on (icao24, observed_at)
      │  dbt: staging → sessionization → marts
      ▼
  FastAPI ──► Next.js dashboard (MapLibre)

  Dagster orchestrates the transforms and runs the data-quality checks.
```

---

## Run it

No credentials, no account, no network. A fresh clone produces output immediately.

```bash
make demo        # ingester → stdout, against recorded fixtures
```

The full stack:

```bash
docker compose up -d          # Redpanda, ClickHouse, MinIO, topic + bucket setup
make run-replay               # ingest recorded fixtures into Kafka
make sink                     # consume into Parquet + ClickHouse
make transform                # dbt build (models + 38 tests)
make api & make web           # http://localhost:3000
```

Pointing it at the live API is one environment variable (`CONTRAIL_SOURCE=live`).
Nothing in the poll loop changes.

---

## The constraint that shaped the design

OpenSky meters `/states/*` with a **daily credit allowance**, not a request rate,
and prices each call by the **area** it covers:

| bounding box | credits |
|---|---|
| ≤ 25 sq° | 1 |
| ≤ 100 sq° | 2 |
| ≤ 400 sq° | 3 |
| larger, or global | 4 |

A registered account gets 4,000 credits/day. A global poll costs 4, so the naive
design (poll the world on a timer) buys **1,000 polls a day, one every 86
seconds**, and nothing else. Spend greedily and ingestion dies before noon.

Two consequences drive the ingester:

**The budgeter paces itself.** It divides unspent credits by the time left before
the quota refills and derives an interval from that. A run that loses hours to a
crash automatically speeds up to use the credits it did not burn.

**The scheduler ranks regions by yield per credit.** Area and value are almost
unrelated. A 6 sq° window over London costs 1 credit and returns several hundred
aircraft; a 400 sq° window over open ocean costs 3 and returns almost none. From a
real replay run:

| region | credits | aircraft | per credit |
|---|---|---|---|
| rhine-alps | 1 | 277 | **277** |
| benelux | 1 | 208 | 208 |
| london | 1 | 206 | 206 |
| paris | 1 | 81 | 81 |
| **iberia** | **3** | **0** | **0** |

### The bug that ranking alone creates

Ranking purely on yield per credit starves quiet regions without bound. The test
that exposed it:

```
polls: london=498  nordics=2      (out of 500)
```

Nominally "not starved". Actually **one observation every six hours**. You cannot
reconstruct a flight from samples that far apart, so those credits bought nothing
at all. A poll rate below what sessionization needs is worse than no coverage,
because it still costs.

The fix is a staleness ceiling: past 10 minutes, a region jumps the queue
regardless of economics. Both regimes are pinned by tests:

| | london | nordics | worst gap |
|---|---|---|---|
| with ceiling | 1967 | 33 | **10m10s** |
| without | 498 | 2 | unbounded |

---

## What the data actually looks like

Every number below is produced by `make fixture-stats`, which runs in CI so the
claims cannot drift from the fixtures they describe.

### `sensors` is null on 100% of vectors

The API documents field 12 as non-nullable. Across 12,292 recorded vectors it was
null **every single time**. A strict decoder fails on every row; the null-tolerant
path is the only reason real responses parse at all.

### Duplicates concentrate on the ground

Consecutive polls overlap almost entirely. An aircraft whose transponder has not
reported since the last poll returns an identical `time_position`: the same
observation, not a new one.

| population | duplicate rate |
|---|---|
| on ground | **38.5%** |
| airborne | **6.6%** |
| overall | 14.7% |

Parked aircraft stop refreshing position while remaining contactable. Dedup is
therefore a **correctness** measure for counts, not a storage optimisation.

### Position staleness reaches 91 minutes

`last_contact - time_position`:

```
p50      0s        p99     101s
p90      1s        max    5497s   ← 91 minutes
```

2.8% of vectors carry a fix older than 10 seconds. This is why observations are
timestamped from `time_position`, never from the response envelope, and why
Parquet partitions on observation time.

The effect is directly visible. In one run, aircraft `4b4b30` was returned by a
poll at **15:48** carrying a fix from **14:17**. It lands alone in `hour=14`:

```
obs_hour   rows   max_stale_min
      14      1            91.1
      15    944            30.0
```

Partitioned by arrival time, that row would sit in `hour=15` and place the
aircraft an hour and a half from where it actually was.

---

## Reconstructing flights

A session boundary is drawn on two signals:

1. **A ground-to-air transition**. Unambiguous: the aircraft took off.
2. **An observation gap beyond 30 minutes**. Ambiguous, and deliberately set high.
   Regional coverage rotates under a 10-minute ceiling, so gaps of that order are
   routine and mean nothing. Too low and one flight shatters into several; too
   high and a turnaround merges two.

Sessions are numbered by a running sum of boundary flags per aircraft, then
aggregated into `fct_flights`, with endpoints matched to airports by proximity.

Every flight is an **inference**, not a record, so the model publishes its own
confidence: `is_complete` (both ends observed on the ground) and
`reconstruction_quality`. From a 10-minute capture over the Benelux:

```
242 flights · 234 aircraft · 27.5 observations each

quality   flights   avg obs   worst gap
medium        199      31.8        871s
low            30       6.0        987s
high           13      10.2        181s
```

Sample output, which validates against reality; the airline/hub pairings are all
correct:

```
callsign   dep    arr    mins   alt_m   kts   dist_km
KLM23R     EHAM   ···    10.6    7635   386      75.9
EWG3AM     EDDL   ···     9.4    7559   431      85.1
RYR7PF     ···    EBCI   10.5    3871   295      67.7
THY1HZ     ···    EBBR   10.5    1623   238      23.5
```

Phase split across all observations: cruise 50.9%, climb 21.9%, descent 20.5%,
ground 6.6%.

---

## Decisions and trade-offs

**Nullable measurements stay nullable, end to end.** An aircraft parked at a gate
genuinely reports `velocity: 0`; one with no fix reports nothing. 10-20% of
vectors are missing altitude, vertical rate or squawk. Collapsing absent to zero
is unrecoverable and biases every average downstream, so the distinction survives
from the Go pointer types through Arrow to ClickHouse `Nullable` columns.

**Deduplication is delegated to the storage engine.** `ReplacingMergeTree` sorted
on `(icao24, observed_at)` makes the sort key the observation key, so dedup is a
property of the table rather than state the consumer must carry across restarts.
The cost is that collapsing is *eventual*: between merges the table holds
duplicates, so the staging model aggregates explicitly rather than trusting
`count()`. `FINAL` would force it at read time but scans far more data.

**The warehouse erases the evidence of its own deduplication.** Once merges
complete, `count()` and `uniqExact` agree by construction and the duplicate rate
reads as exactly zero. It is only observable *at ingestion*, which is why the
sink writes `ingest_batches`, preserving a measurement that would otherwise be
irrecoverable.

**The sink is at-least-once, deliberately.** Offsets commit only after a batch is
durably written, so a crash between write and commit replays the batch and
produces duplicates. The alternative loses data on the same crash. Duplicates are
already handled; lost observations are not.

**Records are keyed by ICAO24.** Sessionization walks one aircraft's positions in
order, so per-aircraft ordering is the one guarantee it cannot do without. Global
ordering is neither needed nor affordable.

**Kafka is here for decoupling, not throughput.** At roughly 110 events/second
this is not a scale problem. The broker buys replay, backpressure, and the ability
to add consumers without touching the ingester, worth being explicit about,
because the throughput argument would be dishonest.

**Airports are a curated seed of ~85 European fields.** Small enough that
nearest-airport matching can be a cross join with `argMin`. Against the full
~80k-row worldwide dataset this shape would need a geospatial index instead.

---

## Testing

| suite | count | notes |
|---|---|---|
| Go | 61 | ~86% coverage; budgeter and scheduler use injected clocks |
| Python | 28 | round-trips real ingester output through the schema |
| dbt | 38 | models, uniqueness, ranges, referential tests |

CI runs `gofmt`/`vet`/`go test -race`, `ruff`/`mypy --strict`/`pytest`, the
fixture measurements, and a full **end-to-end job**: real stack in Docker,
fixtures ingested through Kafka into ClickHouse, `dbt build`, then an assertion
that data actually moved, because a green dbt run over an empty warehouse proves
nothing.

Replay mode exists so all of this works with no credentials and no network.
Recorded fixtures are served through the same `Source` interface as the live
client, so nothing downstream can tell the difference.

---

## Layout

```
ingester/          Go: OAuth2, credit budgeter, region scheduler, Kafka producer
pipeline/
  contrail/        Kafka consumer, schema validation, Parquet + ClickHouse writers
  transform/       dbt: staging → int_flight_sessions → marts
  orchestration/   Dagster assets, schedules, data-quality checks
  clickhouse/init/ Bronze DDL
api/               FastAPI over the gold marts
web/               Next.js + MapLibre dashboard
fixtures/          45 continuous polls (sessionization) + scheduler-demo/
scripts/           fixture_stats.py, the source of every number above
```

## Credentials

Anonymous access works and needs no account (400 credits/day, 10s resolution).
For 4,000 credits/day at 5s resolution, create an API client at
[opensky-network.org/my-opensky/api-clients](https://opensky-network.org/my-opensky/api-clients)
and set `CONTRAIL_OPENSKY_CLIENT_ID` / `_SECRET`.

Note that username/password basic auth was **removed on 18 March 2026**; only the
OAuth2 client credentials flow works, which most tutorials and client libraries
have not caught up with.
