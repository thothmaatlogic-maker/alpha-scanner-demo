# Alpha Scanner — durable processing checkpoint

This isolated feature adds a SQLite-backed event-processing checkpoint. It does not modify the existing demo UI, connect to a live market, or place orders.

## Guarantees
- Output, evidence record, and durable cursor are committed in one `BEGIN IMMEDIATE` transaction.
- WAL mode and `synchronous=FULL` are enabled.
- A retry after a committed transaction returns the original output instead of duplicating it.
- Events in a stream cannot be skipped or consumed before their availability timestamp.
- Output rows are append-only and the integrity verifier checks the evidence hash chain and cursor/output linkage.

## Run
Requires Python 3.10+ and no external packages:

```bash
python -m unittest discover -s tests -v
```

Tests include 16 concurrent calls from 8 workers and a child process that exits abruptly between output insertion and checkpoint persistence. These are local synthetic-data tests, not a proof of safety under every storage failure or a production market integration.
