# Remote classification

Remote classification sends selected local image bytes to an explicitly chosen
HTTPS OpenAI-compatible endpoint. It is optional. Existing local classification
continues to require loopback endpoints. There is no automatic fallback, provider
retry, iCloud download or broad remote queue selection.

Plan and execute are separate commands. Prepare a private JSON array containing
the exact archive asset IDs to consider, with at most 1000 unique IDs and 1 MiB
of JSON. Both this file and the plan must be owner-only regular files, in
owner-only directories without ACL entries. Symlinks in private file paths are
rejected. Every ancestor must be owned by the current user or root and prohibit
group/other writes. Granting ACL entries are rejected on ancestors; macOS deny
entries used by standard system/home directories are allowed. The final private
parent must have mode 0700 with no ACL entries. Directory descriptors remain
pinned and identity/permissions are rechecked to reject replacement or drift.
Private input reads also recheck file device, inode, mode, UID, GID, size and ACL
after the bounded read, rejecting ownership or group changes during reading.
Unknown or deleted IDs fail planning.

```sh
photoscrawl classify --db "$archive" --remote-dry-run \
  --remote-model 'provider/exact-model-id' \
  --remote-model-url 'https://models.example/v1' \
  --asset-ids-file "$private_dir/asset-ids.json" \
  --remote-plan-out "$private_dir/upload-plan.json" --json
```

Planning reads no bearer token, makes no HTTP requests and does not change the
archive queue. The output path must be new; existing files are never replaced.
The private manifest records every requested asset as `planned-upload`,
`unavailable` or `skipped`, including a reason. Upload entries bind source and
archive identity, queue state and fingerprint, the observed claim owner and
nullable claim expiry, resource identity, kind,
designation, SHA256 and byte count, model, endpoint and prompt version. Inspect
this private manifest before authorizing execution. It contains sensitive
selection metadata; keep it outside source repositories and shared directories.
The current manifest format is version 2. Claim fields are observed read-only;
planning creates no queue nonce or reservation. Version 1 plans must be replaced
through fresh planning.

Supply the bearer token through `PHOTOSCRAWL_MODEL_API_KEY`, or use
`--remote-model-key-env NAME` to select another environment variable. A token
must contain 1 to 4096 printable ASCII bytes without whitespace. No token flag,
credential file or credential command is supported.

```sh
photoscrawl classify --db "$archive" \
  --remote-plan "$private_dir/upload-plan.json" \
  --allow-model-upload --limit 25 --json
```

Execution takes selection, model and destination exclusively from the manifest.
Do not pass model, URL, asset-selection or plan-output flags with execution.
`--limit` selects a deterministic prefix of upload entries in manifest order;
when omitted or zero it selects all planned uploads, up to 1000. Other
dispositions are never promoted, even if an image becomes available. Already
classified entries are skipped. Other source, fingerprint, eligibility or
resource changes fail validation before the first request, and each entry is
rechecked when claimed. The observed claim owner and exact nullable expiry must
still match, including when the planned claim was expired; the claim update
enforces that same fence. The same bounded bytes whose digest is verified are
used to build its request. Each selected ID is attempted at most once per
invocation, including failures. Plans expire 24 hours after creation.

To retry selected recent `content_failed` entries, include
`--retry-content-failed` when planning. That policy is bound into the manifest;
it cannot be added or overridden during execution and never requeues unrelated
assets. Without it, the existing seven-day failure cooldown applies.

Remote mode rejects `--all`, local model flags and `--allow-icloud-downloads`.
Completion and reasoning flags retain their existing names, defaults and
environment variables and work with remote OpenAI-compatible requests too.
Configuration or possession of a token alone never grants upload permission.

An endpoint must use HTTPS with normal certificate verification. Bare origins
normalize to `/v1/chat/completions`; `/v1` appends `/chat/completions`. Custom
paths must end in `/chat/completions`. Model IDs are preserved exactly. URL
credentials, queries, fragments and control characters are forbidden. Ambient
proxies and all redirects are disabled. Explicit internal, tailnet and public
HTTPS destinations are permitted; destination choice is the caller's decision.

Raw images are limited to 16 MiB, encoded requests to 25 MiB and responses to
1 MiB. Reads are bounded and detect size changes and oversized growth. Files
without readable regular local image resources are unavailable; oversized
resources are skipped during planning. No download is attempted.

Remote success requires one stopped, non-refusal completion containing exactly
the ten typed fields in the tracked
[remote prompt](../prompts/remote-multimodal-observations-v1.md). This dedicated
prompt keeps anonymous people descriptions and uncertain place candidates while
making schema validation strict; it does not change the local prompt or parser.
A finite policy check rejects decoded strings containing, case-insensitively,
`return only valid compact`, `do not use markdown fences`,
`use candidates, not truth` or `keys:`. It performs exact substring checks,
without fuzzy matching or broader moderation claims.

Unsuccessful remote output is discarded. Provider bodies, refusals, malformed
content and rejected policy text never enter queue reasons, evidence, run
metadata or command output. Only local failure codes, numeric HTTP status and
validated numeric usage/allowlisted finish reasons can be retained. Successful
raw responses and exact provenance remain private archive evidence; command
output contains aggregate counts only.

Remote evidence uses `remote_multimodal`, `network_scope=remote` and
`local_only=false`. Transmission knowledge is reported consistently in command
results, per-asset evidence and run metadata. `cloud_transmitted=false` means
zero attempted uploads. `null` means an upload was possible but confirmation is
missing. `true` requires the HTTP transport's successful `WroteRequest` callback
for the complete request and body, plus a returned HTTP response. This confirms
the local transport handoff; it does not independently prove wire delivery,
provider receipt or upstream processing. A response alone is insufficient.
Transport errors, including DNS/TLS failures, dropped responses and cancellation
before a response, remain unknown even if a write callback fired. This policy
deliberately keeps some actual uploads unknown rather than overstating proof.
`upload_may_have_started` remains true for every invoked or pending attempt.
`confirmed_payload_writes` counts attempts meeting the confirmation rule;
`unknown_transmission_attempts` counts the rest. Any unknown attempt keeps an
aggregate `cloud_transmitted=null`, even alongside confirmed writes. Zero-attempt
skips add neither count and do not erase prior uncertainty.
Observation replacement is scoped to the endpoint, model, prompt and resource
digest. Local, other-endpoint and prior-digest observations and evidence remain,
and each invocation retains a unique run history. Existing archive JSON stores
this evidence without a schema migration.

Before HTTP, the queue claim and a conservative `attempt-started` audit record
commit together. Pending audit metadata has `audit_complete=false`,
`upload_may_have_started=true` and `cloud_transmitted=null` (unknown), with an
empty completion timestamp. Its request count is a conservative bound;
`confirmed_request_attempts` counts invoked requests with a recorded terminal
outcome, not confirmed payload writes; `pending_attempts` counts unfinished audit
entries.
Finalization
records the actual safe outcome, including cancellation or storage failure.
Failed finalization returns `remote-audit-failed` and leaves the durable pending
record acknowledging a possible upload; it does not claim exact response
metadata was saved. Audit-start failure prevents HTTP.

Remote model and recovery modes are mutually exclusive. Do not combine remote
classification flags with `--recovery-reservation-owner` or `--recovery-claim-owner`.
