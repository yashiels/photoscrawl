package archive

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/openclaw/crawlkit/store"
)

type RemoteClassifyResult struct {
	remoteTransmission
	Status       string `json:"status"`
	NetworkScope string `json:"network_scope"`
	LocalOnly    bool   `json:"local_only"`
	Planned      int    `json:"planned_uploads"`
	Unavailable  int    `json:"unavailable"`
	Skipped      int    `json:"skipped"`
	Attempts     int    `json:"request_attempts"`
	Responses    int    `json:"http_responses"`
}

type remoteAttempt struct {
	Entry   remotePlanEntry `json:"entry"`
	Outcome remoteOutcome   `json:"outcome"`
}

type remoteTransmission struct {
	CloudTransmitted            *bool `json:"cloud_transmitted"`
	UploadMayHaveStarted        bool  `json:"upload_may_have_started"`
	ConfirmedPayloadWrites      int   `json:"confirmed_payload_writes"`
	UnknownTransmissionAttempts int   `json:"unknown_transmission_attempts"`
}

func summarizeRemoteTransmission(attempts []remoteAttempt) remoteTransmission {
	var summary remoteTransmission
	for _, attempt := range attempts {
		out := attempt.Outcome
		if out.Attempts == 0 {
			continue
		}
		summary.UploadMayHaveStarted = true
		if out.PayloadWriteConfirmed {
			summary.ConfirmedPayloadWrites++
		} else {
			summary.UnknownTransmissionAttempts += out.Attempts
		}
	}
	if summary.UnknownTransmissionAttempts == 0 {
		confirmed := summary.ConfirmedPayloadWrites > 0
		summary.CloudTransmitted = &confirmed
	}
	return summary
}

var errRemoteAudit = errors.New("remote-audit-failed")

func classifyRemote(ctx context.Context, paths Paths, opts ClassifyOptions) (ClassifyResult, error) {
	o := opts.RemoteModel
	if err := o.validate(opts); err != nil {
		return ClassifyResult{}, err
	}
	completion, err := opts.LocalModelCompletion.normalized()
	if err != nil {
		return ClassifyResult{}, err
	}
	now := opts.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	if o.DryRun {
		return planRemote(ctx, paths, opts, now())
	}
	plan, err := readRemotePlan(o.Plan, now())
	if err != nil {
		return ClassifyResult{}, err
	}
	key, err := remoteKey(o.KeyEnv)
	if err != nil {
		return ClassifyResult{}, err
	}
	identity, err := remoteArchiveFileIdentity(paths.Database)
	if err != nil || identity != plan.Archive {
		return ClassifyResult{}, errors.New("archive-changed")
	}
	inspection, err := openArchiveReadOnly(ctx, paths.Database)
	if err != nil {
		return ClassifyResult{}, errors.New("archive-unavailable")
	}
	preclassified := map[string]bool{}
	err = inspection.WithTx(ctx, func(tx *sql.Tx) error {
		for _, entry := range plan.Entries {
			snapshot, err := loadRemoteSelection(ctx, tx, entry.Asset)
			if err != nil {
				return err
			}
			if !snapshot.matchesPlannedClaim(entry) {
				return errors.New("claim-snapshot-changed")
			}
			if entry.Disposition != "planned-upload" {
				continue
			}
			_, data, err := validateRemoteEntry(ctx, tx, entry, plan, now())
			if err != nil {
				if err.Error() == "already-classified" {
					preclassified[entry.Asset] = true
					continue
				}
				return err
			}
			if _, err = remoteRequest(plan.Model, data, completion); err != nil {
				return err
			}
		}
		return nil
	})
	inspection.Close()
	if err != nil {
		return ClassifyResult{}, err
	}
	db, err := openArchiveStore(ctx, paths.Database)
	if err != nil {
		return ClassifyResult{}, errors.New("archive-unavailable")
	}
	defer db.Close()
	owner, err := newClassificationClaimOwner()
	if err != nil {
		return ClassifyResult{}, errors.New("claim-failed")
	}
	defer releaseClassificationClaims(db.DB(), owner)
	runID := stableID("model_run", remoteSource, owner)
	started := now()
	result := ClassifyResult{Classifier: remoteSource, Remote: &RemoteClassifyResult{remoteTransmission: summarizeRemoteTransmission(nil), Status: "completed", NetworkScope: "remote"}}
	client := o.client
	if client == nil {
		client = newRemoteClient()
		defer client.CloseIdleConnections()
	}
	attempts := []remoteAttempt{}
	selected := 0
	for _, entry := range plan.Entries {
		switch entry.Disposition {
		case "unavailable":
			result.Remote.Unavailable++
			continue
		case "skipped":
			result.Remote.Skipped++
			continue
		}
		if opts.Limit > 0 && selected >= opts.Limit {
			continue
		}
		selected++
		result.Remote.Planned++
		outcome := remoteOutcome{Code: "already-classified"}
		if preclassified[entry.Asset] {
			result.Remote.Skipped++
			attempts = append(attempts, remoteAttempt{Entry: entry, Outcome: outcome})
			if err = recordRemoteSkip(ctx, db, runID, plan, completion, attempts, started, now()); err != nil {
				return result, err
			}
			continue
		}
		var selection remoteSelection
		var body []byte
		pending := append(append([]remoteAttempt{}, attempts...), remoteAttempt{Entry: entry, Outcome: remoteOutcome{Code: "attempt-started", Attempts: 1}})
		err = db.WithTx(ctx, func(tx *sql.Tx) error {
			identity, err := remoteArchiveFileIdentity(paths.Database)
			if err != nil || identity != plan.Archive {
				return errors.New("archive-changed")
			}
			var data []byte
			selection, data, err = validateRemoteEntry(ctx, tx, entry, plan, now())
			if err != nil {
				return err
			}
			body, err = remoteRequest(plan.Model, data, completion)
			if err != nil {
				return err
			}
			if err = claimRemoteInput(ctx, tx, &selection, entry, owner, now()); err != nil {
				return err
			}
			if err = writeRemoteRun(ctx, tx, runID, plan, completion, pending, started, now()); err != nil {
				return errRemoteAudit
			}
			return nil
		})
		if err != nil {
			if errors.Is(err, errRemoteAudit) {
				return result, errRemoteAudit
			}
			outcome.Code = "pre-upload-changed"
			result.Remote.Skipped++
			attempts = append(attempts, remoteAttempt{Entry: entry, Outcome: outcome})
			if err = recordRemoteSkip(ctx, db, runID, plan, completion, attempts, started, now()); err != nil {
				return result, err
			}
		} else {
			attempts = pending
			outcome = callRemote(ctx, client, plan.Endpoint, key, body)
			attempts[len(attempts)-1].Outcome = outcome
			result.Remote.Attempts += outcome.Attempts
			result.Remote.Responses += outcome.Responses
			result.Remote.remoteTransmission = summarizeRemoteTransmission(attempts)
			if ctx.Err() != nil {
				attempts[len(attempts)-1].Outcome.Code = "cancelled"
				attempts[len(attempts)-1].Outcome.success = nil
				if err = finalizeRemoteAudit(ctx, db, runID, plan, completion, attempts, started, now()); err != nil {
					return result, err
				}
				return result, errors.New("remote-cancelled")
			}
			written := 0
			err = db.WithTx(ctx, func(tx *sql.Tx) error {
				current, err := lockCurrentClassificationClaim(ctx, tx, selection.input, now())
				if err != nil {
					return err
				}
				if !current {
					return errors.New("claim-lost")
				}
				written, err = writeRemoteClassification(ctx, tx, selection.input, plan, entry, runID, outcome, now())
				if err != nil {
					return err
				}
				return writeRemoteRun(ctx, tx, runID, plan, completion, attempts, started, now())
			})
			if err != nil {
				outcome.Code = "storage-failed"
				outcome.success = nil
				attempts[len(attempts)-1].Outcome = outcome
				if err = finalizeRemoteAudit(ctx, db, runID, plan, completion, attempts, started, now()); err != nil {
					return result, err
				}
				return result, errors.New("remote-storage-failed")
			}
			result.Processed++
			if outcome.success != nil {
				result.ContentClassified++
				result.ContentObservationsWritten += written
			} else {
				result.ContentClassificationFailures++
			}
		}
	}
	if err = finalizeRemoteAudit(ctx, db, runID, plan, completion, attempts, started, now()); err != nil {
		return result, err
	}
	return result, nil
}

func claimRemoteInput(ctx context.Context, tx *sql.Tx, selection *remoteSelection, entry remotePlanEntry, owner string, now time.Time) error {
	var expiry any
	if entry.ClaimExpiresAt != nil {
		expiry = *entry.ClaimExpiresAt
	}
	claimed, err := tx.ExecContext(ctx, `update classification_queue set claim_owner=?,claim_expires_at=?
where id=? and asset_id=? and input_fingerprint=? and state=? and updated_at=? and claim_owner=? and claim_expires_at is ?
and (claim_owner='' or claim_expires_at is null or julianday(claim_expires_at)<=julianday(?))`, owner, now.Add(classificationLeaseDuration).Format(time.RFC3339Nano), selection.input.QueueID, entry.Asset, entry.Fingerprint, entry.State, entry.Updated, entry.ClaimOwner, expiry, now.Format(time.RFC3339Nano))
	if err != nil {
		return errors.New("claim-failed")
	}
	n, err := claimed.RowsAffected()
	if err != nil || n != 1 {
		return errors.New("claim-conflict")
	}
	selection.input.ClaimOwner = owner
	return nil
}

func finalizeRemoteAudit(ctx context.Context, db *store.Store, runID string, plan remotePlan, completion CompletionConfig, attempts []remoteAttempt, started, completed time.Time) error {
	recordCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := db.WithTx(recordCtx, func(tx *sql.Tx) error {
		return writeRemoteRun(recordCtx, tx, runID, plan, completion, attempts, started, completed)
	}); err != nil {
		return errRemoteAudit
	}
	return nil
}

func recordRemoteSkip(ctx context.Context, db *store.Store, runID string, plan remotePlan, completion CompletionConfig, attempts []remoteAttempt, started, completed time.Time) error {
	err := db.WithTx(ctx, func(tx *sql.Tx) error {
		last := attempts[len(attempts)-1]
		if _, err := writeRemoteEvidence(ctx, tx, plan, last.Entry, runID, last.Outcome, completed); err != nil {
			return err
		}
		return writeRemoteRun(ctx, tx, runID, plan, completion, attempts, started, completed)
	})
	if err != nil {
		return errRemoteAudit
	}
	return nil
}
