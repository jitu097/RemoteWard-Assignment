# RemoteWard Data Pipeline

An event-driven file ingestion pipeline. An Express API streams uploaded CSV/JSON files into S3 and announces
them on SQS. A separate, Dockerised worker (run on Kubernetes) consumes the queue, streams the file back from S3,
computes statistics, and stores status and results in Redis, which the API serves back.

> **Verification status, read this first.** Everything below was built and exercised against a **local AWS
> emulator ([moto](https://github.com/getmoto/moto)) and a real Redis**, chosen so the project costs nothing to run.
> **It has not been run against real AWS.** The AWS SDK code is endpoint-agnostic, but the real-AWS setup in
> [section 7](#7-aws-setup-real-aws) and the IAM policies are untested. See [Known limitations](#18-known-limitations).

| Area | Status |
|---|---|
| API, worker, processors, Redis state, rate limiting | Implemented; unit tested (mocked) and integration tested (real Redis + moto) |
| Docker image | Built and run locally; graceful SIGTERM verified |
| Kubernetes (minikube) | Deployed with 2 worker replicas; processing, Redis-outage resilience, pod deletion verified (against moto) |
| End-to-end (API → moto S3/SQS → k8s worker → Redis → API) | Verified manually, including a malformed file, duplicate events and enqueue-failure recovery |
| **Real AWS (S3, SQS, IAM)** | **Not verified** |
| Load / production-scale traffic | **Not tested** |

## 1. Overview

- `POST /api/v1/uploads` accepts a CSV or JSON file, streams it to S3 (multipart for large files, never fully
  buffered), then publishes a small metadata event to SQS, and returns `202` immediately.
- The worker long-polls SQS, downloads the object as a stream, parses it with streaming parsers, and stores
  deterministic statistics in Redis.
- `GET /api/v1/jobs/:jobId` and `/result` report status and results.
- Upload and read endpoints are rate limited with counters shared through Redis.

## 2. Architecture

```
                    ┌──────────────┐  1. stream   ┌──────────┐
  client ─ upload ─▶│  Express API │─────────────▶│ S3 bucket│
        ◀─ 202 ─────│ (rate limit) │              └────▲─────┘
                    └──┬────────┬──┘                   │ 5. GET object (stream)
            2. publish │        │ 3. job=queued        │
            (metadata) ▼        ▼                      │
                    ┌──────┐  ┌─────────┐        ┌─────┴──────────────┐
                    │ SQS  │  │  Redis  │◀───────│  Worker (k8s, N    │
                    │queue │  │ status  │ 6. lock│  replicas, Docker) │
                    └──┬───┘  │ results │  result│                    │
            4. long-poll│     │ locks   │  status└─────┬──────────────┘
                       └─────▶│ rate-lim│              │ permanent failures
                              └────▲────┘              ▼
  client ── GET job/result ────────┘ (via API)      ┌──────┐
                                                    │ DLQ  │◀─ SQS redrive after N receives
                                                    └──────┘
```

The API and worker share code (`src/services`, `src/utils`) but are independent processes with separate entry
points. The same image can run either (`node src/api/server.js` or `node src/worker/main.js`).

## 3. Technology stack

Node.js 22 (ES modules), Express 5, `busboy` (multipart parsing), AWS SDK v3 (`client-s3`, `client-sqs`,
`lib-storage`), `ioredis`, `express-rate-limit` + `rate-limit-redis`, `csv-parse`, `stream-json`; tests with the
built-in `node:test` runner and `supertest`; ESLint; Docker; Kubernetes (minikube).

**Why not multer-s3?** Version 3.x does support SDK v3 (it wraps `@aws-sdk/lib-storage`), but this project uses
`busboy` + `lib-storage` directly so the size limit, content sniffing and "publish only after S3 completes" ordering
are explicit and testable.

## 4. End-to-end workflow

1. Client `POST`s multipart form data (field name is arbitrary; first file part is used).
2. API validates extension, declared MIME type and the first bytes (content sniffing) and enforces the size limit
   **while streaming** to S3 (`uploads/YYYY/MM/DD/<jobId>.<ext>`, SSE-S3 encryption).
3. API creates the job in Redis (`queued`), then publishes the event to SQS, then responds `202`.
4. Worker receives the message, validates it, takes a per-job Redis lock, marks the job `processing`.
5. Worker streams the object from S3 through the CSV or JSON parser, accumulating statistics.
6. Worker atomically stores the result **and** marks the job `completed`, then deletes the SQS message.
7. Client polls `GET /api/v1/jobs/:jobId`, then `GET /api/v1/jobs/:jobId/result`.

## 5. Repository structure

```
src/
  api/        app.js, server.js, routes/, controllers/, middleware/ (error handler, file guard, rate limit)
  worker/     main.js (entry, health server, signals), consumer.js (poll loop + message handling),
              processor.js, parsers/ (csv, json, stats, stream)
  services/   s3, sqs, redis clients; job.service.js (state machine, Lua scripts, locks)
  config/     env.js (validated configuration)
  utils/      logger.js, errors.js, events.js (event contract)
k8s/          redis.yaml, worker-config.yaml, worker-deployment.yaml
scripts/      setup-local.js (create emulator resources), requeue-job.js (recovery tool)
tests/        unit/ (mocked), integration/ (real Redis; moto for S3/SQS), fixtures/
Dockerfile, .dockerignore, .env.example
```

`src/worker/main.js` is the process entry point; `consumer.js` holds the testable consumer logic.

## 6. Prerequisites

- Node.js ≥ 22 and npm
- Docker (for Redis and the moto emulator, and for the worker image)
- For Kubernetes: `kubectl` and [minikube](https://minikube.sigs.k8s.io/)
- For real AWS only: an AWS account and the AWS CLI (**may incur charges; a free tier is not a guarantee**)

## 7. AWS setup (real AWS)

> **Untested.** These steps were written from the AWS documentation but never executed by the author. Review
> before running, and remember that resources left running can incur charges. Delete them when finished (section 19).

```bash
REGION=us-east-1
BUCKET=<globally-unique-bucket-name>
aws s3api create-bucket --bucket $BUCKET --region $REGION          # add --create-bucket-configuration outside us-east-1
aws s3api put-public-access-block --bucket $BUCKET --public-access-block-configuration \
  BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
aws s3api put-bucket-encryption --bucket $BUCKET --server-side-encryption-configuration \
  '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'
aws s3api put-bucket-lifecycle-configuration --bucket $BUCKET --lifecycle-configuration \
  '{"Rules":[{"ID":"abort-incomplete-mpu","Status":"Enabled","Filter":{"Prefix":""},"AbortIncompleteMultipartUpload":{"DaysAfterInitiation":1}}]}'

aws sqs create-queue --queue-name remoteward-uploads-dlq
DLQ_ARN=$(aws sqs get-queue-attributes --queue-url <dlq-url> --attribute-names QueueArn --query Attributes.QueueArn --output text)
aws sqs create-queue --queue-name remoteward-uploads --attributes \
  "{\"VisibilityTimeout\":\"120\",\"RedrivePolicy\":\"{\\\"deadLetterTargetArn\\\":\\\"$DLQ_ARN\\\",\\\"maxReceiveCount\\\":\\\"3\\\"}\"}"
```

Then create IAM identities (roles preferred; on Kubernetes use IRSA/EKS Pod Identity rather than access keys).
Do **not** use administrator credentials.

### IAM permissions

API (`<bucket>` and `<queue-arn>` are yours):

```json
{ "Version": "2012-10-17", "Statement": [
  { "Effect": "Allow", "Action": ["s3:PutObject", "s3:AbortMultipartUpload"], "Resource": "arn:aws:s3:::<bucket>/uploads/*" },
  { "Effect": "Allow", "Action": "sqs:SendMessage", "Resource": "<queue-arn>" } ] }
```

Worker:

```json
{ "Version": "2012-10-17", "Statement": [
  { "Effect": "Allow", "Action": "s3:GetObject", "Resource": "arn:aws:s3:::<bucket>/uploads/*" },
  { "Effect": "Allow", "Action": ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:ChangeMessageVisibility"], "Resource": "<queue-arn>" },
  { "Effect": "Allow", "Action": "sqs:SendMessage", "Resource": "<dlq-arn>" } ] }
```

The lifecycle rule above (a bucket setting, not an IAM permission) removes abandoned multipart uploads.

## 8. Environment variables

Copy `.env.example` to `.env` (git-ignored). The apps validate configuration at startup and list every problem.

| Variable | Default | Purpose |
|---|---|---|
| `PORT`, `LOG_LEVEL`, `NODE_ENV` | 3000, info | API port and logging |
| `TRUST_PROXY` | 0 | Number of reverse proxies in front of the API (affects client IP for rate limiting) |
| `AWS_REGION` | required | |
| `AWS_ENDPOINT_URL` | unset | Emulator endpoint. **Leave unset for real AWS** |
| `S3_FORCE_PATH_STYLE` | false | `true` for the emulator |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | – | Dummy values for the emulator only; with real AWS prefer a profile or role |
| `S3_BUCKET`, `SQS_QUEUE_URL`, `SQS_DLQ_URL` | required | |
| `SQS_VISIBILITY_TIMEOUT_SECONDS` | 120 | Must exceed `JOB_LOCK_TTL_SECONDS` |
| `SQS_MAX_RECEIVE_COUNT` | 3 | Used by `setup-local`; real queues carry their own redrive policy |
| `REDIS_URL` | required | |
| `JOB_STATUS_TTL_SECONDS` / `JOB_RESULT_TTL_SECONDS` | 86400 | 24 h |
| `JOB_LOCK_TTL_SECONDS` | 60 | Renewed every TTL/3 while processing |
| `MAX_UPLOAD_BYTES` | 10485760 | 10 MiB |
| `UPLOAD_RATE_LIMIT_MAX` / `_WINDOW_MS` | 30 / 900000 | Per IP per 15 min |
| `READ_RATE_LIMIT_MAX` / `_WINDOW_MS` | 60 / 60000 | Per IP per minute |
| `WORKER_CONCURRENCY` | 2 | Max in-flight jobs per worker |
| `WORKER_SHUTDOWN_TIMEOUT_MS` | 30000 | Drain time on SIGTERM |
| `WORKER_HEALTH_PORT` | 8080 | `/healthz`, `/readyz` |

## 9. Local setup (API + worker on your machine)

PowerShell, with Docker Desktop running:

```powershell
docker run -d --name rw-moto -p 5000:5000 motoserver/moto:latest
docker run -d --name rw-redis -p 6379:6379 redis:7-alpine
Copy-Item .env.example .env
npm ci
npm run setup:local      # creates bucket, queue and DLQ in the emulator (refuses to run without AWS_ENDPOINT_URL)
npm run start:worker     # terminal 1
npm run start:api        # terminal 2
```

If port 8080 is taken on your machine, set `WORKER_HEALTH_PORT=8081`. Check: `curl http://localhost:3000/health`.

## 10. Docker

```powershell
docker build -t remoteward-worker:1.0.0 .
docker run --rm --name rw-worker `
  -e AWS_REGION=us-east-1 -e AWS_ENDPOINT_URL=http://host.docker.internal:5000 -e S3_FORCE_PATH_STYLE=true `
  -e AWS_ACCESS_KEY_ID=test -e AWS_SECRET_ACCESS_KEY=test -e S3_BUCKET=remoteward-uploads `
  -e SQS_QUEUE_URL=http://host.docker.internal:5000/123456789012/remoteward-uploads `
  -e SQS_DLQ_URL=http://host.docker.internal:5000/123456789012/remoteward-uploads-dlq `
  -e REDIS_URL=redis://host.docker.internal:6379 remoteward-worker:1.0.0
```

The image is `node:22-alpine`, installs production dependencies only (`npm ci --omit=dev`), runs as the non-root
`node` user, has a healthcheck, and runs `node` as PID 1 so SIGTERM triggers graceful shutdown. Secrets are never
baked in. It is single-stage because a second stage would save nothing (no build step).

> Use `host.docker.internal` (not `host.minikube.internal`) for the emulator: moto misparses the latter's
> hostname as an S3 bucket name. This is an emulator quirk and irrelevant for real AWS.

## 11. Kubernetes (minikube)

```powershell
minikube start --driver=docker --cpus=2 --memory=2048
minikube image load remoteward-worker:1.0.0
# Credentials are created at deploy time and never stored in the repo. These are DUMMY values for the emulator:
kubectl create secret generic aws-credentials --from-literal=AWS_ACCESS_KEY_ID=test --from-literal=AWS_SECRET_ACCESS_KEY=test
kubectl apply -f k8s/redis.yaml -f k8s/worker-config.yaml -f k8s/worker-deployment.yaml
kubectl rollout status deploy/worker
kubectl get pods
kubectl logs -l app=worker --prefix --tail=20
kubectl scale deploy/worker --replicas=3
```

Notes:

- The worker is a long-lived **Deployment** (2 replicas), not a Job. `terminationGracePeriodSeconds: 45` exceeds the
  30 s shutdown drain.
- **Liveness** probes only the process and its poll loop, never Redis or AWS, so a dependency outage does not
  restart pods; the worker reconnects and retries on its own (verified with a 45 s Redis outage: 0 restarts).
- Redis has one replica with an append-only file on a 1 GiB PVC so job state survives pod restarts.
- To run the API against the in-cluster Redis from your machine:
  `kubectl port-forward svc/redis 6380:6379`, then start the API with `REDIS_URL=redis://localhost:6380`.
- For real AWS: remove `AWS_ENDPOINT_URL` and `S3_FORCE_PATH_STYLE` from `k8s/worker-config.yaml`, set the real
  bucket/queue URLs, and prefer an IAM role for the pod over the Secret.

## 12. API

Responses are JSON. Errors use `{ "success": false, "error": { "code": "...", "message": "..." } }`.

```powershell
curl.exe -F "file=@tests/fixtures/customers.csv;type=text/csv" http://localhost:3000/api/v1/uploads
```

```json
{ "success": true, "message": "File accepted for processing",
  "data": { "jobId": "…", "status": "queued", "fileName": "customers.csv", "statusUrl": "/api/v1/jobs/…" } }
```

```powershell
curl.exe http://localhost:3000/api/v1/jobs/<jobId>          # status
curl.exe http://localhost:3000/api/v1/jobs/<jobId>/result   # result
```

| Endpoint | Outcomes |
|---|---|
| `GET /health` | 200 (process liveness only) |
| `POST /api/v1/uploads` | 202; 400 `NO_FILE`/`EMPTY_FILE`/`MALFORMED_UPLOAD`; 413 `FILE_TOO_LARGE`; 415 `UNSUPPORTED_FILE_TYPE`; 429; 503 `ENQUEUE_FAILED`/`STORAGE_UNAVAILABLE`/`JOB_STORE_UNAVAILABLE` |
| `GET /api/v1/jobs/:jobId` | 200; 400 invalid id; 404 `JOB_NOT_FOUND` (unknown or expired) |
| `GET /api/v1/jobs/:jobId/result` | 200; 404; 409 `RESULT_NOT_READY` (not completed); 410 `RESULT_EXPIRED` |

Result schema (identical for CSV and JSON apart from `fileType`):

```json
{ "fileType": "csv", "totalRecords": 4, "totalColumns": 5, "columns": ["id","name","age","city","balance"],
  "emptyValues": {"id":0,"name":0,"age":1,"city":1,"balance":0},
  "numericStats": {"age": {"count":3,"min":29,"max":41,"sum":104,"mean":34.666667}} }
```

### Processing rules and limitations of the statistics

- Columns appear in first-seen order. "Empty" = `null`/missing/blank string. A missing JSON key counts as empty.
- A column gets `numericStats` only if **every** non-empty value is numeric. CSV values are strings and are parsed
  as numbers; JSON strings are never coerced.
- Sums/means are rounded to 6 decimals; floating-point accumulation error is not corrected.
- Malformed input fails the **whole job** (no partial results): ragged CSV rows, duplicate/blank headers,
  invalid JSON, non-array JSON root, non-object elements. Max 1000 distinct columns.
- Nested JSON objects/arrays count as non-empty, non-numeric values and are not flattened.
- No duplicate-record detection is performed; it was out of scope.
- CSV/JSON must be UTF-8 (BOM tolerated); UTF-16 is rejected.

### Sample files

`tests/fixtures/customers.csv` and `tests/fixtures/orders.json` (the latter has a `null`, an empty string and a missing key).

## 13. Event contract (`FILE_UPLOADED`, version 1)

```json
{ "eventType": "FILE_UPLOADED", "eventVersion": 1, "jobId": "<uuid>",
  "file": { "bucket": "…", "key": "uploads/2026/11/24/<jobId>.csv", "originalName": "customers.csv",
            "contentType": "text/csv", "size": 524288 },
  "uploadedAt": "ISO-8601" }
```

Metadata only; file contents are never sent through SQS. `contentType` is `text/csv` or `application/json`.
The worker validates every message (`src/utils/events.js`). A malformed or unsupported-version event is a
permanent failure. Changing the shape requires a new `eventVersion`.

## 14. Job lifecycle

| State | Meaning |
|---|---|
| `queued` | Stored in S3, event published |
| `processing` | A worker holds the job |
| `completed` | Result stored. **Terminal**: nothing can leave this state |
| `failed` | Last attempt failed (`error.retryable` says whether SQS will redeliver) |
| `enqueue_failed` | Stored in S3 but the SQS publish failed |

Transitions: `queued→processing`, `processing→completed`, `processing→failed`, `failed→processing` (retry),
`processing→processing` (re-claim after a crashed worker), `queued→enqueue_failed`, `enqueue_failed→queued`
(operator recovery). All transitions are atomic Redis Lua scripts; refused transitions leave the record untouched.

## 15. Failure handling and retry design

- **S3 and SQS are separate systems; there is no transaction between them.** The API uploads first, records the
  job, then publishes. The event is never sent before the upload completes.
- **Publish fails after upload:** the API returns 503 (never "queued"), keeps the object, marks the job
  `enqueue_failed`, and logs `upload.enqueue_failed` with the job id, bucket and key. **Recovery:**
  `npm run requeue -- <jobId>` re-publishes the event. If Redis is also down, the log line has the S3 key.
- **At-least-once delivery:** SQS Standard may deliver a message more than once. Duplicates are handled by the
  per-job lock plus the terminal `completed` state; this is idempotent handling, **not** exactly-once processing.
  A duplicate that hits the lock backs off and is redelivered after the visibility timeout.
- **Transient failures** (S3/network/Redis errors): the failure is recorded, the message is **not** deleted, and SQS
  redelivers it after the visibility timeout (retry delay = `SQS_VISIBILITY_TIMEOUT_SECONDS`, no backoff). After
  `maxReceiveCount` receives SQS moves it to the DLQ (a queue setting).
- **Permanent failures** (bad event, malformed file, missing object, unknown job): recorded, copied to the DLQ with
  a `failureReason` attribute, then deleted. No pointless retries.
- **The message is deleted only after the outcome is persisted** (result or recorded failure). If the delete fails,
  the redelivery is recognised as a duplicate and deleted then.
- **Long jobs:** a heartbeat renews the lock and extends the message visibility every `lockTTL/3`.
- **Redis outage:** the worker stops polling (so messages don't burn receives toward the DLQ), reconnects with
  backoff, and resumes. The API fails individual requests with 503 and rate limiting fails open.
- **Shutdown:** SIGTERM stops polling and lets in-flight jobs finish within `WORKER_SHUTDOWN_TIMEOUT_MS`; remaining
  work is aborted, the lock released, and the message left for redelivery.

## 16. Redis key structure

| Key | Value | TTL |
|---|---|---|
| `job:status:{jobId}` | JSON job record (status, file metadata, attempts, error, timestamps) | 24 h (refreshed on change) |
| `job:result:{jobId}` | JSON result | 24 h |
| `job:lock:{jobId}` | random holder token | 60 s, renewed while processing |
| `rl:upload:<ip>`, `rl:read:<ip>` | rate-limit counters | window length |

A completed job whose result has expired returns `410 RESULT_EXPIRED`, never a fake result.

## 17. Testing

```powershell
npm run lint
npm test                    # unit tests (mocked S3/SQS/Redis fakes, no services needed)
npm run test:integration    # needs Redis on :6379 and moto on :5000 (see section 9)
```

- `tests/unit`: processors, event validation, file guard, job transitions, config, API behaviour with in-memory
  fakes (valid/invalid/oversized upload, queue failure, Redis failure, job retrieval, missing job/result).
- `tests/integration`: real Redis (job service Lua scripts, locks, rate limiting shared across instances),
  moto S3/SQS (streaming and multipart upload, SQS publish), and the worker with fake S3/SQS over real Redis
  (success, malformed event, S3 failure, Redis persistence failure, duplicates, concurrent duplicates, retry,
  delete-after-persist ordering, bounded concurrency, graceful shutdown).
- **None of the tests talk to real AWS.** Moto is an emulator and may differ from AWS in edge cases.
- Kubernetes behaviour was verified by hand (section 11), not by automated tests.

## 18. Known limitations

- **Not verified on real AWS** (S3 SSE headers, lifecycle rules, redrive policy, IAM policies, IRSA).
- Not load tested; no throughput or scale claims are made.
- Retry delay equals the visibility timeout (no exponential backoff). A job whose messages are dead-lettered stays
  `failed` in Redis.
- Rate limiting fails open if Redis is down; there is no authentication, so limits are per IP only.
- Job state expires after 24 h; an event older than that is treated as an unknown job (permanent failure).
- One Redis instance (no HA). The Kubernetes setup is a local minikube demo, not a production design; there is no
  Ingress, NetworkPolicy, autoscaling or API deployment manifest (the API runs locally).
- Orphaned S3 objects can remain if Redis is unavailable at upload time (the object key is logged).
- Statistics are single-pass with the caveats in section 12.

## 19. Cleanup

```powershell
kubectl delete -f k8s/worker-deployment.yaml -f k8s/worker-config.yaml -f k8s/redis.yaml
kubectl delete secret aws-credentials
minikube delete
docker rm -f rw-moto rw-redis
docker rmi remoteward-worker:1.0.0
```

Real AWS (if you created it): empty and delete the bucket, delete both queues, and delete the IAM roles/policies.
