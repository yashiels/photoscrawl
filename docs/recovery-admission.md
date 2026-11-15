# Bounded classification recovery

Recovery mode processes an explicitly reserved cohort through the existing local,
loopback-only model transport. It does not reserve rows, change upload consent,
renew leases, reclaim owners, clean up claims, or retry ambiguous attempts.

A controller must seal its cohort, input fingerprints, unique participants, and
one fixed expiry before atomically reserving pending rows in
`classification_queue.claim_owner` and `claim_expires_at`. The controller owns
reservation and final disposition; the classifier only transfers exact matching
pending reservations to its participant. No other queue states or owners qualify.

```console
photoscrawl classify --db /path/to/synthetic/photos.sqlite \
  --local-model fixture --local-model-api openai \
  --local-model-url http://127.0.0.1:1234 \
  --recovery-reservation-owner recovery:example-run:reservation \
  --recovery-claim-owner recovery:example-run:worker-1 --limit 25
```

Both owner flags must be supplied together and nonempty. Identifiers have the
form `recovery:<run>:<participant>`. Run names are 1–48 lowercase ASCII letters,
digits, or hyphens, starting with a letter or digit. Participant names follow the
same alphabet with a 1–32 character bound. The reservation participant must be
`reservation`; a worker must use another participant in the same run. Controllers
must assign unique worker names and never reuse them for another invocation.
The API fields are `RecoveryReservationOwner` and `RecoveryClaimOwner` on
`ClassifyOptions`. A positive API `Limit` is explicit; CLI callers must supply
`--limit` themselves. Recovery accepts 1–1000, requires a local model, and rejects
`--all` and iCloud downloads. Existing transport validation excludes remote URLs.
No new environment or authentication settings are involved.

The exact reservation expiry text is preserved during transfer. Admission needs
more than one minute of lease remaining and at most two hours. An invocation pins
its first expiry and rejects any different expiry on subsequent claims. Its
context deadline is the earlier of the caller deadline and 30 seconds before
expiry. Claim admission rejects caller deadlines with 30 seconds or less left;
attempt admission also requires more than one minute before expiry. There is no
lease renewal. Expired reservations produce no eligible work, never a takeover.

Before content access, a separate committed transaction changes owned `pending`
to `recovery_started`, with reason `recovery_attempt_started` and the actual
current timestamp. This means an attempt may have occurred. It is not success or
proof that image bytes were sent. The state fits the existing TEXT schema, with
no migration. Status exposes its count and an explicit incomplete-attempt warning.
Both current and tested 0.5.1 retry selectors exclude it even after expiry.

The first SQL operation in the outcome transaction is a conditional update binding
queue ID, asset ID, started state, participant, fingerprint, exact expiry, and a
live bounded deadline. Exactly one row must match before any metadata, content,
FTS, terms, evidence, model-run, or terminal-state writes. SQLite holds the writer
lock through those writes and commit. A lost guard returns `recovery incomplete`
with `recovery claim lost`; no false successful return occurs. A later storage
failure rolls back the entire outcome to the committed started marker.
Successful outcomes use real `content_classified`, `content_failed`, or
`content_unavailable` states; unavailable content is not silently left retryable.
Model-run accounting is committed with each guarded outcome. An empty cohort
makes zero model requests and writes no recovery model run.

Recovery never releases participant claims on exit, including completed rows,
cancellation, storage errors, crashes, and pre-attempt pending leftovers. An error
can follow earlier committed outcomes; inspect exact rows rather than treating
an invocation result as an atomic batch. Ordinary classification retains its
existing selection, lease, retry, and cleanup behavior.

After proving every owned process has exited, a controller may restore only exact
sealed, still-pending preimages that never crossed the started marker, and may
clear claims on exactly verified completed outcomes. Started or changed rows,
unknown owners, and ambiguous outcomes require an explicit incomplete report,
retained backup, and manual operator disposition. Expiry alone never authorizes
requeueing a started attempt. There is no automatic replay or cleanup mechanism.
