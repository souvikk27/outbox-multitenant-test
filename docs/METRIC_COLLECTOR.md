# Metric Collector — Performance Testing Guide

A practical guide to driving load, collecting telemetry, and reading the four canonical
performance patterns: healthy, noisy-neighbor, DB-bottleneck, handler-bottleneck.

This document assumes the system from `PROJECT_BIBLE.md` is running. If you haven't read that yet,
do — the metric names below are emitted by the components described there.

---

## 1. Quick start

```
# 1. Run a Postgres + OTel stack locally
docker compose -f deploy/observability/docker-compose.yml up -d

# 2. Point the app at OTLP and Postgres
export ConnectionStrings__DefaultConnection="Host=localhost;Database=outbox_db;Username=postgres;Password=postgres"
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
export OTEL_EXPORTER_OTLP_PROTOCOL=grpc

# 3. Apply migrations and start the service
dotnet ef database update --context AppDbContext
dotnet run

# 4. Drive load
curl -X POST "http://localhost:5203/outbox/seed?count=10000"
watch -n 1 'curl -s http://localhost:5203/outbox/status | jq'

# 5. Open Grafana
open http://localhost:3000   # admin/admin
```

The `docker-compose.yml` in §3 stands up Postgres + OpenTelemetry Collector + Prometheus + Grafana
with a pre-provisioned dashboard. All metric names below are exactly what flows through that stack.

---

## 2. The metric catalog

Every metric is emitted on the `Meter` named `OutboxTestInmemory.Outbox`
(see `OutboxDiagnostics.MeterName`). All counters/histograms are tagged so dashboards can slice
by dimension.

### 2.1 Throughput counters

| Metric | Unit | Tags | What it tells you |
|---|---|---|---|
| `outbox.messages.processed` | `{message}` | `event_type`, `tenant_id` | Successful dispatches. The numerator of "throughput". |
| `outbox.messages.failed` | `{message}` | `event_type`, `tenant_id`, `kind` (`transient`\|`permanent`) | Failures by kind. |
| `outbox.messages.retried` | `{message}` | `event_type`, `tenant_id` | Transient failures rescheduled. |
| `outbox.messages.dead_lettered` | `{message}` | `event_type`, `tenant_id` | Moved to `Failed` (gave up). |
| `outbox.messages.claimed` | `{message}` | `worker_id` | Total messages picked up. Sum across workers ≈ `processed + failed + retried`. |
| `outbox.lease.recovered` | `{message}` | — | Stuck `Processing` rows reset by the recovery sweep. |
| `outbox.worker.errors` | `{error}` | `worker_id` | Errors thrown inside the worker loop (DB blip, etc.). |

### 2.2 Latency histograms

| Metric | Unit | Tags | What it measures |
|---|---|---|---|
| `outbox.message.duration` | `ms` | `event_type`, `tenant_id` | **Handler invocation only.** Time inside `IOutboxHandler.HandleAsync`. |
| `outbox.message.queue_delay` | `ms` | `event_type`, `tenant_id` | **Time waiting for a worker.** From `AvailableAt` until the dispatcher picks the message up. Includes claim-queue + worker-fan-out delays. Spikes = ingestion outpacing claim. |
| `outbox.claim.duration` | `ms` | `worker_id`, `batch_size` | **Time inside `ClaimBatchAsync`.** The `FOR UPDATE SKIP LOCKED` SQL round-trip. |
| `outbox.complete.duration` | `ms` | `batch_size` | **Time inside `BulkCompleteAsync`.** Per-mutation `ExecuteUpdateAsync` calls inside one transaction. |

### 2.3 What's NOT in the meter (and where to get it)

- **Backlog depth** — not a `Meter` instrument; query `GET /outbox/status` (returns `OutboxBacklog`)
  or scrape Postgres directly with `postgres_exporter` (see §3.4 for the SQL).
- **Lag (created_at → processed_at)** — derive from `outbox.message.queue_delay + outbox.message.duration`
  for the post-due window. For full end-to-end including pre-due delay, query the table directly.

### 2.4 Derived signals

Most dashboards/alerts work on derived rates and percentiles. The canonical PromQL forms:

```promql
# Throughput (msg/sec)
sum(rate(outbox_messages_processed_total[1m]))

# Per-tenant throughput
sum by (tenant_id) (rate(outbox_messages_processed_total[1m]))

# Per-event-type throughput
sum by (event_type) (rate(outbox_messages_processed_total[1m]))

# Handler p95 (ms)
histogram_quantile(0.95, sum by (le) (rate(outbox_message_duration_milliseconds_bucket[5m])))

# Handler p95 per tenant
histogram_quantile(0.95, sum by (le, tenant_id) (rate(outbox_message_duration_milliseconds_bucket[5m])))

# Queue delay p95
histogram_quantile(0.95, sum by (le) (rate(outbox_message_queue_delay_milliseconds_bucket[5m])))

# Claim duration p95 (the DB-side claim cost)
histogram_quantile(0.95, sum by (le) (rate(outbox_claim_duration_milliseconds_bucket[5m])))

# Bulk-complete duration p95
histogram_quantile(0.95, sum by (le) (rate(outbox_complete_duration_milliseconds_bucket[5m])))

# Failure ratio (transient + permanent / processed)
sum(rate(outbox_messages_failed_total[1m]))
  /
ignoring(kind) sum(rate(outbox_messages_processed_total[1m]))

# Permanent-failure ratio (the alarm signal)
sum(rate(outbox_messages_failed_total{kind="permanent"}[5m]))
  /
sum(rate(outbox_messages_processed_total[5m]))

# Lease recovery rate (any sustained value > 0 means workers are dying)
sum(rate(outbox_lease_recovered_total[5m]))
```

OTLP-to-Prometheus naming converts dots to underscores and appends `_total` to counters. If your
backend keeps native OTLP names, drop those suffixes.

---

## 3. Setup

### 3.1 Local docker-compose stack

Save as `deploy/observability/docker-compose.yml`:

```yaml
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: outbox_db
    ports: ["5432:5432"]
    volumes:
      - pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "postgres"]
      interval: 5s

  otel-collector:
    image: otel/opentelemetry-collector-contrib:0.110.0
    command: ["--config=/etc/otelcol/config.yaml"]
    ports:
      - "4317:4317"   # OTLP gRPC (app pushes here)
      - "4318:4318"   # OTLP HTTP
      - "8889:8889"   # Prometheus scrape endpoint (collector exposes)
    volumes:
      - ./otel-collector.yaml:/etc/otelcol/config.yaml:ro

  prometheus:
    image: prom/prometheus:v2.55.0
    ports: ["9090:9090"]
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml:ro

  grafana:
    image: grafana/grafana:11.3.0
    ports: ["3000:3000"]
    environment:
      GF_AUTH_ANONYMOUS_ENABLED: "true"
      GF_AUTH_ANONYMOUS_ORG_ROLE: Admin
    volumes:
      - ./grafana-datasources.yml:/etc/grafana/provisioning/datasources/datasources.yml:ro

volumes:
  pgdata:
```

`otel-collector.yaml`:
```yaml
receivers:
  otlp:
    protocols:
      grpc: { endpoint: 0.0.0.0:4317 }
      http: { endpoint: 0.0.0.0:4318 }

processors:
  batch:
    timeout: 5s

exporters:
  prometheus:
    endpoint: 0.0.0.0:8889
  debug:
    verbosity: basic

service:
  pipelines:
    metrics:
      receivers:  [otlp]
      processors: [batch]
      exporters:  [prometheus]
    traces:
      receivers:  [otlp]
      processors: [batch]
      exporters:  [debug]
```

`prometheus.yml`:
```yaml
global:
  scrape_interval: 5s
scrape_configs:
  - job_name: otel-collector
    static_configs:
      - targets: ["otel-collector:8889"]
```

`grafana-datasources.yml`:
```yaml
apiVersion: 1
datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
```

### 3.2 App-side configuration

Already wired in `Program.cs`:
```csharp
builder.Services
    .AddOpenTelemetry()
    .ConfigureResource(r => r.AddService("OutboxTestInmemory"))
    .WithMetrics(m => m
        .AddMeter(OutboxDiagnostics.MeterName)
        .AddAspNetCoreInstrumentation()
        .AddRuntimeInstrumentation()
        .AddOtlpExporter())
    .WithTracing(t => t
        .AddSource(OutboxDiagnostics.ActivitySourceName)
        .AddAspNetCoreInstrumentation()
        .AddOtlpExporter());
```

The OTLP exporter reads endpoint from the `OTEL_EXPORTER_OTLP_ENDPOINT` env var.

### 3.3 Optional: Prometheus exporter (no collector)

For the simplest setup, swap OTLP for the Prometheus AspNetCore exporter:
```
dotnet add package OpenTelemetry.Exporter.Prometheus.AspNetCore
```
```csharp
.WithMetrics(m => m
    .AddMeter(OutboxDiagnostics.MeterName)
    .AddPrometheusExporter())
// In the app:
app.MapPrometheusScrapingEndpoint(); // /metrics
```
Then point Prometheus directly at the app on `/metrics`. Skip the collector entirely. Use this for
dev/laptop iteration; OTLP is the right answer in production where you want one telemetry pipe.

### 3.4 Backlog gauges from Postgres

The library doesn't emit backlog gauges (they'd require sync-over-async in the meter callback or
a separate observer service). For a Prometheus-native gauge, run `postgres_exporter` against your
DB and add a `queries.yaml`:

```yaml
outbox_backlog:
  query: |
    SELECT
      COUNT(*) FILTER (WHERE status = 0) AS pending,
      COUNT(*) FILTER (WHERE status = 1) AS processing,
      COUNT(*) FILTER (WHERE status = 3) AS failed,
      EXTRACT(EPOCH FROM (NOW() - MIN(created_at) FILTER (WHERE status = 0)))::int AS oldest_pending_age_seconds
    FROM outbox_messages
  metrics:
    - pending: { usage: GAUGE, description: "Pending messages" }
    - processing: { usage: GAUGE, description: "Currently being processed" }
    - failed: { usage: GAUGE, description: "Dead-lettered" }
    - oldest_pending_age_seconds: { usage: GAUGE }
```

This gives you `outbox_backlog_pending` etc. as first-class gauges.

---

## 4. Driving load

### 4.1 The seed endpoint

`POST /outbox/seed?count=N` (max 10000 per call) inserts N email events spread across three
tenants. It uses `IOutboxPublisher.Enqueue + SaveChangesAsync`, so it exercises the same producer
path real code uses.

### 4.2 k6 scripts

Save as `scripts/k6-steady.js`:
```javascript
import http from 'k6/http';
import { sleep } from 'k6';

export const options = {
  scenarios: {
    steady: {
      executor: 'constant-arrival-rate',
      rate: 5,                  // 5 batches/sec
      timeUnit: '1s',
      duration: '5m',
      preAllocatedVUs: 10,
      env: { COUNT: '500' },    // 500 events per batch -> 2500 events/sec sustained
    },
  },
};

export default function () {
  http.post(`http://localhost:5203/outbox/seed?count=${__ENV.COUNT}`);
}
```

Run:
```
k6 run scripts/k6-steady.js
```

### 4.3 Spike test

```javascript
// scripts/k6-spike.js
import http from 'k6/http';
export const options = {
  scenarios: {
    spike: {
      executor: 'ramping-arrival-rate',
      startRate: 1,
      timeUnit: '1s',
      preAllocatedVUs: 50,
      stages: [
        { target: 1,   duration: '30s' },   // baseline
        { target: 100, duration: '30s' },   // spike
        { target: 100, duration: '2m'  },   // sustain
        { target: 1,   duration: '1m'  },   // recover
      ],
    },
  },
};
export default () => http.post('http://localhost:5203/outbox/seed?count=500');
```

Use spike to find the saturation point and observe how queue_delay vs claim_duration evolve.

### 4.4 Direct SQL seeding (for higher rates)

The HTTP endpoint is rate-limited by ASP.NET Core's request pipeline. To stress beyond ~5k msg/sec
ingest, bypass it:

```sql
INSERT INTO outbox_messages
  (id, tenant_id, event_type, payload, status, retry_count, available_at, created_at)
SELECT
  gen_random_uuid(),
  (ARRAY['tenant-a','tenant-b','tenant-c'])[1 + floor(random() * 3)::int],
  'email',
  jsonb_build_object('to', 'u' || g || '@example.com', 'subject', 'h', 'body', 'b', 'index', g)::text,
  0, 0, NOW(), NOW()
FROM generate_series(1, 100000) g;
```

This loads 100k messages instantly. The processing side is the bottleneck under test.

### 4.5 Skewed-tenant load (for noisy-neighbor)

```sql
-- 95% of load to tenant-a, 5% split across the other two
INSERT INTO outbox_messages (id, tenant_id, event_type, payload, status, retry_count, available_at, created_at)
SELECT
  gen_random_uuid(),
  CASE WHEN random() < 0.95 THEN 'tenant-a'
       WHEN random() < 0.5  THEN 'tenant-b'
       ELSE 'tenant-c' END,
  'email',
  jsonb_build_object('to', 'x', 'subject', 'h', 'body', 'b', 'index', g)::text,
  0, 0, NOW(), NOW()
FROM generate_series(1, 50000) g;
```

This is the input you use to validate the noisy-neighbor diagnostic in §6.

### 4.6 Methodology

For any benchmark run, capture:

1. **Warm-up** — first 30s after starting load is misleading (cold caches, JIT, EF model build).
   Discard.
2. **Steady-state window** — 3–5 minutes of stable load. Record min/p50/p95/p99/max for each
   latency histogram.
3. **Drain time** — stop ingestion, time how long it takes for `pending` to reach 0. This is your
   effective drain rate.
4. **Resource ceiling** — record CPU%, memory, DB connections, DB CPU%, DB IOPS. The bottleneck
   is whichever saturates first.
5. **Post-mortem** — query the DB for failure distribution:
   ```sql
   SELECT status, retry_count, COUNT(*)
   FROM outbox_messages
   GROUP BY status, retry_count ORDER BY status, retry_count;
   ```

Parameterize `WorkerCount`, `BatchSize`, `MaxTenantConcurrency` between runs. A simple grid search
of (workers ∈ {2,4,8,16}, batch ∈ {50,100,200,500}) usually finds the sweet spot in 16 runs.

---

## 5. The signals you actually care about

In order of usefulness for spotting problems:

1. **`outbox.message.queue_delay` p95** — single best leading indicator. If queue_delay is rising,
   ingestion is outpacing claim+process. Everything else follows.
2. **`outbox.messages.processed` rate** — the throughput number you report.
3. **`outbox.message.duration` p95** — handler health. If this rises and queue_delay rises with it,
   it's the handler. If queue_delay rises and duration is flat, it's claim/scheduling.
4. **`outbox.claim.duration` p95** — DB read-side health.
5. **`outbox.complete.duration` p95** — DB write-side health (often dominates over claim because
   each batch produces N updates).
6. **`outbox.messages.failed{kind="permanent"}` rate** — alert signal. Permanent failures aren't
   self-healing.
7. **`outbox.lease.recovered` rate** — any sustained value > 0 means workers are crashing
   mid-batch. Investigate.
8. **`outbox_backlog_pending`** (from postgres_exporter) — long-term health. If this trends up
   over hours, the system isn't actually keeping up.

---

## 6. What good looks like — the four patterns

### 6.1 Healthy system

**Signature**:
- `outbox.messages.processed` rate is **stable** at the expected throughput.
- `outbox.message.duration` p95 < **500 ms** (or whatever your handler SLO is).
- `outbox.message.queue_delay` p95 < **200 ms**.
- Per-tenant processed rates are **proportional to ingestion rates** (no tenant starved).
- `outbox.claim.duration` p95 < **20 ms**.
- `outbox.complete.duration` p95 < **50 ms** (scales with `BatchSize`).
- `outbox.messages.failed{kind="permanent"}` ≈ 0.
- `outbox.lease.recovered` ≈ 0.
- `outbox_backlog_pending` flat or oscillating in a tight range.

**Validation queries**:
```promql
# Throughput stability — coefficient of variation across the window
stddev_over_time(sum(rate(outbox_messages_processed_total[1m]))[10m:1m])
  /
avg_over_time(sum(rate(outbox_messages_processed_total[1m]))[10m:1m])
# Healthy: < 0.15
```

```promql
# Tenant balance — ratio of max-tenant rate to mean-tenant rate
max by () (sum by (tenant_id) (rate(outbox_messages_processed_total[5m])))
  /
avg by () (sum by (tenant_id) (rate(outbox_messages_processed_total[5m])))
# Healthy: < 1.5x given balanced ingestion. Higher = either skewed input or scheduling problem.
```

If you're running the seed endpoint with the default uniform distribution, all four signals
above should be in their healthy bands within ~30 s of starting.

---

### 6.2 Noisy neighbor

One tenant dominates a worker (or all workers) and others wait.

**Signature**:
- One `tenant_id` shows **high `outbox.messages.processed` rate** AND **high
  `outbox.message.duration`**.
- Other tenants show **rising `outbox.message.queue_delay` p95** (they're stuck behind the slow
  tenant in the per-tenant FIFO).
- Other tenants show **low or zero `outbox.messages.processed` rate** despite their backlogs
  growing.
- Aggregate `outbox.claim.duration` is fine.
- Aggregate `outbox.complete.duration` is fine.

**PromQL**:
```promql
# Per-tenant queue delay p95 — the smoking gun
histogram_quantile(0.95,
  sum by (le, tenant_id) (rate(outbox_message_queue_delay_milliseconds_bucket[5m])))
```
A noisy neighbor shows one tenant flat and low while others climb.

```promql
# Per-tenant duration ratio
histogram_quantile(0.95,
  sum by (le, tenant_id) (rate(outbox_message_duration_milliseconds_bucket[5m])))
```
The noisy tenant will be 10–100× the others.

**Why it happens**: `OutboxProcessor.ProcessBatchAsync` groups the claimed batch by tenant and
runs partitions in parallel up to `MaxTenantConcurrency`. WITHIN one tenant's partition, messages
run sequentially (preserves per-tenant FIFO). If one tenant has many slow messages and dominates
the claimed batch, the partition takes a long time and other tenants in subsequent batches wait.

**Remediation**:
1. **Increase `MaxTenantConcurrency`** so more tenants can run in parallel within a batch. Cheap
   first step.
2. **Reduce `BatchSize`** so the batch composition turns over faster — slow tenant gets less of
   each batch, fast tenants don't wait as long for the next claim.
3. **Increase `WorkerCount`** so multiple workers each see a different mix.
4. **For severe cases**: split the slow tenant into its own outbox/worker pool (different
   `EventType` namespace + dedicated handler). Or rate-limit the noisy producer upstream.
5. **Diagnostic**: run §4.5's skewed-tenant SQL seed and observe the signature reproduce.

---

### 6.3 DB bottleneck

The database is the limit; handlers are idle waiting for work.

**Signature**:
- `outbox.claim.duration` p95 **spikes** (claim SQL is slow).
- AND/OR `outbox.complete.duration` p95 **spikes** (writes are slow).
- `outbox.message.duration` p95 is **low** (handlers are fast when they get work).
- Per-worker throughput **plateaus** despite increasing `WorkerCount`.
- Postgres `pg_stat_activity` shows many connections in `active` state on UPDATEs.
- Postgres CPU % high; connection-pool waits in DB metrics.

**PromQL**:
```promql
# Where is time being spent?
histogram_quantile(0.95, sum by (le) (rate(outbox_claim_duration_milliseconds_bucket[5m])))
histogram_quantile(0.95, sum by (le) (rate(outbox_complete_duration_milliseconds_bucket[5m])))
histogram_quantile(0.95, sum by (le) (rate(outbox_message_duration_milliseconds_bucket[5m])))

# DB time as % of total per-message time:
# (claim_p95 / batch_size + complete_p95 / batch_size) / message_duration_p95
# Healthy: < 20%. DB-bottlenecked: > 50%.
```

**Why it happens**:
- `BatchSize` too large → `BulkCompleteAsync` issues N sequential UPDATEs in one transaction; long
  transactions hold row locks.
- `WorkerCount × instances` exceeds Postgres `max_connections` capacity.
- `outbox_messages` table bloated with `Processed` rows → claim CTE plan changes.
- Index missing or dropped (verify `ix_outbox_messages_pending_due` exists).
- Other workload sharing the DB.

**Remediation**:
1. **Verify indexes** are present:
   ```sql
   \d+ outbox_messages
   -- expect ix_outbox_messages_pending_due ... WHERE status = 0
   ```
2. **Vacuum `Processed` rows** if you don't archive them:
   ```sql
   DELETE FROM outbox_messages WHERE status = 2 AND processed_at < NOW() - INTERVAL '7 days';
   VACUUM ANALYZE outbox_messages;
   ```
   In production, schedule this; the partial indexes mean Processed bloat doesn't slow the hot
   path, but it does slow `GROUP BY status` (backlog query).
3. **Reduce `BatchSize`** to shorten transaction duration (less lock holding).
4. **Increase Postgres `max_connections`** OR introduce a connection pool (PgBouncer transaction
   mode).
5. **Scale Postgres up** (CPU, IOPS) before scaling workers further. Adding workers when DB is
   saturated makes things worse, not better.
6. **Profile the slow query** with `EXPLAIN ANALYZE`:
   ```sql
   EXPLAIN (ANALYZE, BUFFERS)
   WITH cte AS (
     SELECT id FROM outbox_messages
     WHERE status = 0 AND available_at <= NOW()
     ORDER BY available_at, created_at
     LIMIT 100 FOR UPDATE SKIP LOCKED
   )
   UPDATE outbox_messages m SET status = 1 FROM cte WHERE m.id = cte.id;
   ```
   Should be Index Scan on `ix_outbox_messages_pending_due`. If it's a Seq Scan, the index is
   missing or stats are stale.

---

### 6.4 Handler bottleneck

The handler is the limit; the DB and workers are idle waiting for it to finish.

**Signature**:
- `outbox.message.duration` p95 **high**.
- `outbox.claim.duration` p95 **low**.
- `outbox.complete.duration` p95 **low**.
- `outbox.messages.processed` rate is **flat** even when adding workers.
- `outbox.message.queue_delay` p95 climbs (handlers can't keep up with claim throughput).
- Handler-side resource saturation: app CPU high, downstream service latency high, etc.

**PromQL**:
```promql
# Handler share of per-message latency
# Should be ~80%+ of total. If it's >95% AND queue_delay is climbing, the handler IS the bottleneck.

histogram_quantile(0.95, sum by (le) (rate(outbox_message_duration_milliseconds_bucket[5m])))
  /
ignoring(le) histogram_quantile(0.95, sum by (le) (
  rate(outbox_message_duration_milliseconds_bucket[5m])
  + rate(outbox_message_queue_delay_milliseconds_bucket[5m])
))
```

**Why it happens**:
- Handler does synchronous IO to a slow downstream service (SMTP, third-party API).
- Handler does excessive work per message (large CPU computation).
- Downstream service is rate-limiting or backing off the handler's calls.

**Remediation**:
1. **Add downstream observability**. The dispatcher already creates one `Activity` per message;
   instrument the handler internally to attribute time within. Look at trace waterfall.
2. **Increase `WorkerCount`** if the bottleneck is IO-bound (more concurrent waiters help).
   `WorkerCount × MaxTenantConcurrency` is the in-flight handler concurrency ceiling.
3. **Increase `BatchSize`** so workers spend more time inside the batch's parallel fan-out and
   less time idling on poll cycles. Helps if handler is fast individually but you see worker
   gaps.
4. **For CPU-bound handlers**: scale horizontally (more pods), not vertically. Workers are async
   so they don't benefit from more cores per pod.
5. **Decompose**: split slow handlers into a fast "schedule" step and a slower "execute" step
   handled by a different event type.
6. **Cap the handler timeout** (`HandlerTimeoutSeconds`) so a hung downstream doesn't lock a
   worker indefinitely. The default 5 s is reasonable; raise only if your SLO requires it and
   you've sized the worker pool accordingly.

---

## 7. Tuning playbook

The settings live in `appsettings.json` under `Outbox`. See `OutboxOptions.cs` for the full schema
and validated ranges.

| Symptom | Knob to try first | Why |
|---|---|---|
| Throughput plateaus, CPU low, DB low | `WorkerCount ↑` | Add concurrency. |
| Throughput plateaus, CPU high | `WorkerCount` already saturated; scale pods horizontally | More processes. |
| `claim.duration` ↑ | `BatchSize ↓` OR check DB capacity | Smaller claim is faster; long claim → long lease. |
| `complete.duration` ↑ | `BatchSize ↓` OR add DB IOPS | Per-mutation update fan-out grows with batch. |
| `queue_delay` ↑, `duration` flat | `WorkerCount ↑` or `PollIntervalMs ↓` | Workers under-subscribed. |
| `queue_delay` ↑, `duration` ↑ | Handler bottleneck — see §6.4 | Process side. |
| One tenant dominates | `MaxTenantConcurrency ↑` first, then `BatchSize ↓` | Force fan-out to other tenants. |
| `lease.recovered` > 0 sustained | Investigate worker crashes; raise `LeaseTimeoutSeconds` only if handlers legitimately need it | Don't paper over the crash. |
| Permanent failures spiking | Inspect `last_error`; the classifier may be misjudging an exception type | App correctness, not throughput. |
| Backlog growing over hours | True throughput < ingestion. Scale pods OR move slow handlers to async | Fundamental capacity gap. |
| `IdleBackoffMs` high & traffic bursty | `IdleBackoffMs ↓` | Reduce wake-up latency at cost of idle DB chatter. |

**The tuning order matters**: start with `WorkerCount` and `BatchSize` (cheap, in-process). Move to
`MaxTenantConcurrency` for tenant skew. Touch DB sizing only after the in-process settings are
exhausted. Touch `PollIntervalMs`/`IdleBackoffMs` last — they trade latency for DB load.

---

## 8. Reference benchmark scenarios

Run these to characterize a deployment. Capture the same metric panels for each.

### 8.1 Steady-state throughput

- **Setup**: defaults except `WorkerCount=4`, `BatchSize=100`.
- **Load**: §4.2 k6 script at 2500 msg/sec for 5 minutes.
- **Capture**: throughput rate, `duration` p50/p95/p99, `queue_delay` p95, `claim.duration` p95,
  `complete.duration` p95, DB CPU%.
- **Pass**: throughput sustained, `queue_delay` p95 < 200 ms, no failed/lease-recovered.

### 8.2 Saturation point

- **Setup**: defaults.
- **Load**: §4.3 ramping spike to 10× steady-state.
- **Capture**: when does `queue_delay` p95 cross 1 s? When does throughput stop scaling?
- **Pass**: documented saturation point, system recovers within 60 s after spike ends.

### 8.3 Noisy neighbor

- **Setup**: defaults.
- **Load**: §4.5 skewed seed (95% one tenant), 50k messages.
- **Capture**: per-tenant queue_delay p95 and processed rate.
- **Pass**: small tenants drain within 2× the time the big tenant takes (NOT proportional to
  message count alone).

### 8.4 Worker crash recovery

- **Setup**: 8 workers, 5k message backlog.
- **Action**: SIGKILL the process while running. Restart.
- **Capture**: `lease.recovered` rate after restart; verify all messages eventually `Processed`.
- **Pass**: zero lost messages (`pending + processing + processed = total`); recovery within
  `LeaseTimeoutSeconds + LeaseRecoveryIntervalSeconds`.

### 8.5 DB outage

- **Setup**: defaults, steady load.
- **Action**: stop Postgres for 30 s, restart.
- **Capture**: `outbox.worker.errors` during outage; behavior when DB returns.
- **Pass**: workers back-off and log, don't crash; throughput resumes within 30 s of DB recovery.

### 8.6 Per-knob sensitivity sweep

For each axis, hold all others at default, vary the one and record steady-state throughput.

| Axis | Values to sweep |
|---|---|
| `WorkerCount` | 1, 2, 4, 8, 16, 32 |
| `BatchSize` | 25, 50, 100, 200, 500 |
| `MaxTenantConcurrency` | 1, 2, 4, 8, 16 |
| `PollIntervalMs` | 100, 250, 500, 1000, 2000 |

Output: a 2D plot of throughput vs the swept value. The knee of each curve identifies your
deployment-specific sweet spot.

---

## 9. Failure-mode validation

Run these manually before trusting the system in prod.

| Scenario | How to inject | What to verify |
|---|---|---|
| Permanent handler error | Modify `EmailHandler` to always throw `ArgumentException` | `kind=permanent` failure; row goes straight to `Failed`, no retries. |
| Transient handler error | Already simulated (`TimeoutException` ~25%) | `kind=transient`; `retried` counter increments; `available_at` set in the future with jitter. |
| Handler timeout | Have handler `await Task.Delay(TimeSpan.FromSeconds(60))` | Timeout fires at `HandlerTimeoutSeconds`; classified as `Cancelled` (default classifier) → returns to `Pending`. |
| Worker crash | `kill -9 $(pgrep -f OutboxTestInmemory)` mid-batch | After `LeaseTimeoutSeconds`, recovery sweep resets stuck rows. |
| DB unreachable | `docker stop postgres-container` for 30 s | EF retry-on-failure absorbs first attempts; worker logs errors and backs off; throughput resumes when DB returns. |
| Two pods, one DB | Run two `dotnet run` against same DB | No double-processing (verify by ProcessedAt timestamps and counts). Throughput approximately 2×. |
| Schema drift | Drop `ix_outbox_messages_pending_due`; observe | `claim.duration` p95 spikes 10–100×. The signal works. |

---

## 10. Cheat sheet

| If you see... | Look at... | Probable cause |
|---|---|---|
| `queue_delay` p95 ↑ | `duration` and `claim.duration` p95 | One of those is the actual bottleneck |
| `claim.duration` ↑, `complete.duration` flat | DB read path | Index, stats, or vacuum issue |
| `claim.duration` flat, `complete.duration` ↑ | DB write path | Lock contention, IOPS, or batch size |
| `duration` ↑ uniformly across tenants | Handler or downstream | Outside the library |
| `duration` ↑ for one tenant only | That tenant's handler workload | Producer or domain |
| `lease.recovered` ↑ | Worker errors log | Crashes or pod restarts |
| `messages.failed{kind=permanent}` ↑ | `last_error` column | App bug or upstream contract change |
| `processed` rate flat, no errors | Backlog gauge | Either no work to do, or claim is starving (check `claim.duration`) |
| `worker.errors` ↑ | App logs | DB connectivity or unhandled exception path |
