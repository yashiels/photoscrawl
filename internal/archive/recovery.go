package archive

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

const recoveryDeadlineMargin = 30 * time.Second
const recoveryAdmissionMargin = time.Minute

var recoveryOwnerPattern = regexp.MustCompile(`^recovery:([a-z0-9][a-z0-9-]{0,47}):([a-z0-9][a-z0-9-]{0,31})$`)

func validateRecoveryOptions(opts ClassifyOptions) error {
	if opts.RecoveryReservationOwner == "" && opts.RecoveryClaimOwner == "" {
		return nil
	}
	reservation := recoveryOwnerPattern.FindStringSubmatch(opts.RecoveryReservationOwner)
	participant := recoveryOwnerPattern.FindStringSubmatch(opts.RecoveryClaimOwner)
	if reservation == nil || participant == nil || reservation[1] != participant[1] || reservation[2] != "reservation" || participant[2] == "reservation" {
		return errors.New("recovery owners must be recovery:<run>:reservation and recovery:<same-run>:<unique-participant>")
	}
	if opts.All || opts.Limit <= 0 || opts.Limit > 1000 || opts.AllowICloudDownloads || strings.TrimSpace(opts.LocalModel) == "" {
		return errors.New("recovery requires a local model and positive limit <=1000, without all or iCloud downloads")
	}
	return nil
}

func claimRecoveryInput(ctx context.Context, tx *sql.Tx, opts ClassifyOptions, now time.Time, fixedExpiry string) (*classifyInput, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("recovery incomplete: %w", err)
	}
	var queueID, expiryText string
	err := tx.QueryRowContext(ctx, `
update classification_queue set claim_owner = ?
where id = (
  select q.id from classification_queue q join asset a on a.id = q.asset_id
  where q.state = 'pending' and q.claim_owner = ? and a.deleted_at is null
    and julianday(q.claim_expires_at) > julianday(?)
  order by a.creation_date desc, q.id limit 1
)
returning id, claim_expires_at
`, opts.RecoveryClaimOwner, opts.RecoveryReservationOwner, now.Format(time.RFC3339Nano)).Scan(&queueID, &expiryText)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claim recovery reservation: %w", err)
	}
	expiry, err := time.Parse(time.RFC3339Nano, expiryText)
	if err != nil || expiry.Sub(now) > 2*time.Hour || expiry.Sub(now) <= recoveryAdmissionMargin || (fixedExpiry != "" && fixedExpiry != expiryText) {
		return nil, errors.New("recovery incomplete: reservation requires one fixed expiry, more than 1 minute and at most 2 hours away")
	}
	if deadline, ok := ctx.Deadline(); ok && deadline.Sub(now) <= recoveryDeadlineMargin {
		return nil, errors.New("recovery incomplete: insufficient context deadline margin")
	}
	input, err := loadClaimedClassifyInput(ctx, tx, queueID, opts.RecoveryClaimOwner)
	if err != nil {
		return nil, err
	}
	input.ClaimExpiresAt = expiryText
	return input, nil
}

func recoveryCAS(ctx context.Context, tx *sql.Tx, input classifyInput, now time.Time, expected, next, reason string) error {
	expiry, err := time.Parse(time.RFC3339Nano, input.ClaimExpiresAt)
	if err != nil || !now.Before(expiry.Add(-recoveryDeadlineMargin)) {
		return errors.New("recovery claim lost: fixed deadline elapsed")
	}
	if deadline, ok := ctx.Deadline(); !ok || !now.Before(deadline) || deadline.After(expiry.Add(-recoveryDeadlineMargin)) {
		return errors.New("recovery claim lost: invalid context deadline")
	}
	result, err := tx.ExecContext(ctx, `
update classification_queue
set state = ?, reason = case when ? = '' then reason else ? end,
    updated_at = case when ? = '' then updated_at else ? end
where id = ? and asset_id = ? and state = ? and claim_owner = ?
  and input_fingerprint = ? and claim_expires_at = ?
  and julianday(claim_expires_at) > julianday(?)
`, next, reason, reason, reason, now.Format(time.RFC3339Nano), input.QueueID, input.AssetID, expected, input.ClaimOwner, input.InputFingerprint, input.ClaimExpiresAt, now.Add(recoveryDeadlineMargin).Format(time.RFC3339Nano))
	if err != nil {
		return fmt.Errorf("recovery claim guard: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return errors.New("recovery claim lost: incomplete outcome; preserved current row")
	}
	return nil
}

func markRecoveryStarted(ctx context.Context, tx *sql.Tx, input classifyInput, now time.Time) error {
	if expiry, err := time.Parse(time.RFC3339Nano, input.ClaimExpiresAt); err != nil || expiry.Sub(now) <= recoveryAdmissionMargin {
		return errors.New("recovery claim lost: insufficient attempt margin")
	}
	return recoveryCAS(ctx, tx, input, now, "pending", "recovery_started", "recovery_attempt_started")
}

func guardRecoveryOutcome(ctx context.Context, tx *sql.Tx, input classifyInput, now time.Time) error {
	return recoveryCAS(ctx, tx, input, now, "recovery_started", "recovery_started", "")
}

func writeRecoveryModelRun(ctx context.Context, tx *sql.Tx, recovery bool, result ClassifyResult, classifier localModelClassifier, endpoints map[string]struct{}, claimed int, now time.Time) error {
	if !recovery {
		return nil
	}
	return writeModelRun(ctx, tx, result.ModelRunID, classifier, endpoints, claimed, result.LocalModelHTTPRequestAttempts, result.LocalModelHTTPResponses, result.ContentClassified, result.ContentClassificationFailures, result.LocalModelDiagnostics, now)
}
