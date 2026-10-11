# Once Per Plate

At 11:40 pm an analyst is staring at a 96-well ELISpot plate, re-typing spot counts into a spreadsheet for the fourth night that week. That scene is from Scispot's own [Smart Actions launch post](https://www.ycombinator.com/launches/S4m-scispot-smart-actions). Smart Actions takes that work off her: today she uploads the export, clicks "run analysis", reviews a preview and approves it.

Scispot's [Aug 26, 2026 post by Guru Singh](https://www.scispot.com/blog/smart-actions-automated-lab-workflows) says runs "take around 5–15 minutes on average" and lists a planned next step: "auto-execution, so Smart Actions can trigger automatically when new files arrive."

When the button goes away, the file arriving *is* the trigger. Then the boring failure cases decide whether her Labsheet is right: the same export synced twice, a file still being written, a worker that dies ten minutes into a run, fifty plates landing at once. She can no longer see whether a run fired once, twice, or never. The [Senior Backend & Infrastructure Engineer post](https://www.ycombinator.com/companies/scispot-io/jobs/QX1c0xf-senior-backend-infrastructure-engineer) names the same problem first: "How do we absorb bursts of instrument and workflow data without losing work, creating duplicates, or slowing customer-facing services?"

This repo is a small, working version of that trigger layer, in the stack the post lists (FastAPI, RabbitMQ, Postgres).

**It is a pattern, not a fix.** I have no view into Scispot's code and this says nothing about how Scispot works today. Every export, sample and threshold here is synthetic.

**Video (64 s, silent):** [docs/demo.mp4](docs/demo.mp4) · **Live demo:** https://once-per-plate.onrender.com (free tier, the first load can take a minute)

## The one guarantee

**One plate export, one QC run, one approval, one set of rows.** However many times the file arrives, whichever worker dies, however often RabbitMQ redelivers, and however many times someone clicks Approve.

## Run it (under 5 minutes)

Needs Python 3.11+, plus Postgres and RabbitMQ. With Docker:

```bash
make setup      # venv and dependencies
make services   # Postgres 17 on :5434 and RabbitMQ 4 on :5672 (docker-compose.yml)
make run        # watcher + 3 workers + the page at http://127.0.0.1:8000
make chaos      # in another terminal, with `make run` stopped: the 16 chaos cases
```
`make test` runs the fast unit tests for the parser and QC math (no services needed).

Without Docker, point `DATABASE_URL` and `AMQP_URL` at any Postgres and RabbitMQ (defaults: `postgresql://opp@127.0.0.1:5434/opp`, `amqp://guest:guest@127.0.0.1:5672/%2F`). The chaos script restarts the services, so tell it how with `OPP_PG_RESTART`, `OPP_MQ_STOP` and `OPP_MQ_START` (`make chaos-docker` sets them for the compose setup). I built and ran everything on a box with Postgres 17 and RabbitMQ 4.0.5 installed directly. I did not have Docker there, so the compose file is the one piece I have not run.

## Chaos results

`python scripts/chaos.py` starts real worker processes against real RabbitMQ and Postgres, breaks things, and checks the ledger after each case. Last full run: **16/16 PASS** ([docs/chaos-results.json](docs/chaos-results.json), [docs/chaos-output.txt](docs/chaos-output.txt)).

| Case | What it does | Result |
|---|---|---|
| same_file_twice | same export dropped twice, then the same bytes under a new name | 3 arrivals, 1 run, QC ran once |
| half_written | file written in chunks; then a half file that stalls longer than the ready window, with a checksum sidecar | no run while writing; the sidecar holds the stalled file |
| reexport_changed_well | approved plate re-exported with one well changed | v2 marked "supersedes v1", waits for its own approval |
| kill_worker_mid_run | `kill -9` on the worker holding the run | RabbitMQ redelivers, next worker takes over after the lease, 1 proposal |
| crash_before_ack | worker commits its result, dies before acking | redelivered message is a no-op |
| duplicate_messages | the same run id published 5 extra times | 1 QC run, 5 no-ops |
| broker_restart | RabbitMQ stopped with unacked messages; 3 files arrive while it is down | 9 plates, 9 runs, 9 proposals |
| postgres_restart | Postgres restarted mid-batch | 11 plates, 11 proposals |
| burst_50 | 50 plates plus 10 duplicate re-syncs at once | 60 files, 50 runs, each plate once |
| approve_twice | Approve sent 10 times concurrently | 1 approval applied, 96 rows |
| reject_then_redrop | rejected run, same file dropped again | linked to the rejected run, 0 rows, Approve now returns 409 |
| writeback_timeout | rows commit but the writer never hears back and retries; an approval whose writer died | retry is a no-op; the sweeper finishes the orphan once |
| two_workers_race | 20 threads claim one run at the same instant | 1 wins; losers cannot write a result |
| out_of_order | v2 arrives while v1 is running; then an older export arrives late | v1 result discarded, old export kept for audit only, 1 approvable version |
| malformed_parked | corrupt CSV and random bytes next to a good file | both go to the parked queue after 1 attempt; good file still runs; no worker crashes |
| ledger_replay | mixed approvals and rejections, then rebuild state from the event log | every run's state matches |

After every case the script also checks: runs equal distinct file contents, Labsheet rows equal 96 x written runs, no run produced two proposals, and replaying the event log gives the same states as the runs table.

I checked that the script can fail: with the duplicate lookup in ingest switched off, `same_file_twice` fails, and with the claim ignoring another worker's live lease, `two_workers_race` fails.

## How it works

```
file lands ─► ready? (size stable N s, or .sha256 sidecar matches)
          ─► ledger.ingest  run_key = sha256(bytes) + instrument + plate   (UNIQUE)
               same bytes again  ─► linked to the existing run, nothing new runs
               new bytes, same plate ─► version n+1, "supersedes vn", open older version closed
          ─► RabbitMQ opp.runs (durable, persistent, publisher confirms)
          ─► worker (manual ack, prefetch 1)
               claim: SELECT ... FOR UPDATE SKIP LOCKED + lease (renewed while working)
               QC, then store the proposal only if it still holds the lease
               malformed export ─► reject, dead-lettered to opp.parked
          ─► PROPOSED ─► person approves (row lock) ─► rows INSERT ... ON CONFLICT (run_id, well) DO NOTHING

RECEIVED → QUEUED → RUNNING → PROPOSED → APPROVED → WRITTEN
                        └→ PARKED      └→ REJECTED          any open state → SUPERSEDED
```
- The message is only a hint. The ledger row in Postgres decides, so a duplicate or redelivered message finds the run claimed or finished and does nothing.
- A dead worker's message comes back from RabbitMQ unacked. The next worker sees a live lease, waits for it to expire, then takes over (attempt 2). A worker that finishes after losing its lease has its result discarded.
- The sweeper publishes runs that were recorded while RabbitMQ was down, re-nudges runs whose lease expired, and finishes approvals whose write-back never ran.
- Every state change is an event row with its new state, so the whole ledger can be rebuilt from the log, and each run shows a "why this ran once" line.
- QC is a plain function ([opp/elispot.py](opp/elispot.py)): replicate CV, LLOQ, TNTC, background and positive-control checks, giving Positive, Negative, Indeterminate or QC Fail. No model in the path.

Code: [opp/ledger.py](opp/ledger.py) (all state changes), [opp/worker.py](opp/worker.py), [opp/watcher.py](opp/watcher.py) (watcher and sweeper), [opp/api.py](opp/api.py), [scripts/chaos.py](scripts/chaos.py).

## Real vs simulated

- **Real:** the ledger, RabbitMQ queue semantics (acks, redelivery, dead-lettering), row locks, leases, unique constraints, the chaos script, the QC math.
- **Simulated:** the instrument exports (an invented 96-well format, synthetic donors), storage (a local folder), the Labsheet (a Postgres table), the approver (no login), and the analysis time (about a second instead of minutes).
- **Live demo hosting:** one free Render container that runs Postgres and RabbitMQ inside it next to the app ([start.sh](start.sh)), so the demo runs on real RabbitMQ and Postgres too, but nothing persists across restarts. Its kill button really sends SIGKILL to a worker process. The broker and database restarts only happen in the chaos script. **The chaos results above come from the run against RabbitMQ 4.0.5 and Postgres 17 on my build box, not from the demo.**
- **No load numbers.** The burst case shows correctness under 60 files, not throughput.

## What I'd check with real data

Actual duplicate and partial-upload rates per instrument and storage connector; what "file is ready" means for each instrument (some write in place, some rename, few write checksums); where deduplication lives today; and whether a trigger like this belongs at the sync layer or in front of the action.

Not built: auth, multi-tenant isolation, a real Labsheet API, Kubernetes, cloud deploys. Not affiliated with Scispot. Synthetic data only.
