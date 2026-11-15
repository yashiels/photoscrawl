package archive

import (
	"context"
	"database/sql"
	"strings"
	"time"
)

func remoteNamespace(plan remotePlan, entry remotePlanEntry) string {
	return stableID("remote_namespace", remoteSource, plan.Endpoint, plan.Model, plan.Prompt, entry.SHA256)
}

func writeRemoteClassification(ctx context.Context, tx *sql.Tx, input classifyInput, plan remotePlan, entry remotePlanEntry, runID string, outcome remoteOutcome, at time.Time) (int, error) {
	evidenceID, err := writeRemoteEvidence(ctx, tx, plan, entry, runID, outcome, at)
	if err != nil {
		return 0, err
	}
	namespace := remoteNamespace(plan, entry)
	if outcome.success == nil {
		return 0, updateClassificationQueue(ctx, tx, input.QueueID, "content_failed", outcome.Code, at)
	}
	if err = clearRemoteObservations(ctx, tx, input.AssetID, namespace); err != nil {
		return 0, err
	}
	return writeRemoteObservations(ctx, tx, input, plan, evidenceID, namespace, outcome.success, at)
}

func writeRemoteEvidence(ctx context.Context, tx *sql.Tx, plan remotePlan, entry remotePlanEntry, runID string, outcome remoteOutcome, at time.Time) (string, error) {
	transmission := summarizeRemoteTransmission([]remoteAttempt{{Outcome: outcome}})
	namespace := remoteNamespace(plan, entry)
	evidenceID := stableID("evidence", remoteSource, entry.Asset, plan.Endpoint, plan.Model, plan.Prompt, entry.SHA256)
	value := map[string]any{
		"classifier": remoteSource, "model_id": plan.Model, "prompt_version": plan.Prompt, "requested_endpoint": plan.Endpoint,
		"network_scope": "remote", "local_only": false, "cloud_transmitted": transmission.CloudTransmitted,
		"upload_may_have_started": transmission.UploadMayHaveStarted, "confirmed_payload_writes": transmission.ConfirmedPayloadWrites, "unknown_transmission_attempts": transmission.UnknownTransmissionAttempts,
		"manifest_digest": plan.Digest, "resource": entry, "outcome": outcome, "run_id": runID, "classified_at": at.Format(time.RFC3339Nano),
	}
	if outcome.Responses > 0 {
		value["response_endpoint"] = plan.Endpoint
	}
	if outcome.success == nil {
		evidenceID = stableID("evidence", runID, entry.Asset, "failure")
	} else {
		value["raw_response"] = outcome.success.raw
		value["parsed_response"] = outcome.success.payload
	}
	encoded, err := jsonText(value)
	if err != nil {
		return "", err
	}
	if _, err = tx.ExecContext(ctx, `insert into evidence_ref(id,asset_id,evidence_kind,source,pointer,value_json) values (?,?,?,?,?,?) on conflict(id) do nothing`, evidenceID, entry.Asset, "content_classification", remoteSource, namespace, encoded); err != nil {
		return "", err
	}
	return evidenceID, nil
}

func writeRemoteObservations(ctx context.Context, tx *sql.Tx, input classifyInput, plan remotePlan, evidenceID, namespace string, success *remoteSuccess, at time.Time) (int, error) {
	payload := map[string]any{}
	for key, value := range success.payload {
		if texts, ok := value.([]string); ok {
			items := make([]any, len(texts))
			for i, text := range texts {
				items[i] = text
			}
			payload[key] = items
		} else {
			payload[key] = value
		}
	}
	observations := observationsFromPayload(payload)
	for _, observation := range observations {
		id := stableID("model_observation", input.AssetID, namespace, observation.ObservationType, observation.ValueText)
		encoded, err := jsonText(observation.Value)
		if err != nil {
			return 0, err
		}
		if _, err = tx.ExecContext(ctx, `insert into model_observation(id,asset_id,observation_type,value_text,value_json,confidence,source,model_id,prompt_version,evidence_id) values (?,?,?,?,?,?,?,?,?,?)`, id, input.AssetID, observation.ObservationType, observation.ValueText, encoded, observation.Confidence, remoteSource, plan.Model, plan.Prompt, evidenceID); err != nil {
			return 0, err
		}
		if err = insertObservationFTS(ctx, tx, id, input.AssetID, observation.ValueText, strings.Join([]string{observation.ObservationType, observation.ValueText, remoteSource, plan.Model}, " ")); err != nil {
			return 0, err
		}
		for _, term := range observationTerms(observation) {
			if _, err = tx.ExecContext(ctx, `insert into observation_term(id,asset_id,observation_id,term,term_type,source,model_id) values (?,?,?,?,?,?,?)`, stableID("observation_term", input.AssetID, id, term), input.AssetID, id, term, observation.TermType, remoteSource, plan.Model); err != nil {
				return 0, err
			}
		}
	}
	return len(observations), updateClassificationQueue(ctx, tx, input.QueueID, "content_classified", "remote-model-observations", at)
}

func clearRemoteObservations(ctx context.Context, tx *sql.Tx, asset, namespace string) error {
	selection := `select m.id from model_observation m join evidence_ref e on e.id=m.evidence_id where m.asset_id=? and m.source=? and e.pointer=?`
	for _, query := range []string{
		`delete from observation_fts where rowid in (select fts_rowid from observation_fts_rowid where observation_id in (` + selection + `))`,
		`delete from observation_fts_rowid where observation_id in (` + selection + `)`,
		`delete from observation_term where observation_id in (` + selection + `)`,
		`delete from model_observation where id in (` + selection + `)`,
	} {
		if _, err := tx.ExecContext(ctx, query, asset, remoteSource, namespace); err != nil {
			return err
		}
	}
	return nil
}

func writeRemoteRun(ctx context.Context, tx *sql.Tx, id string, plan remotePlan, completion CompletionConfig, attempts []remoteAttempt, started, completed time.Time) error {
	uploads := 0
	pending := 0
	for _, attempt := range attempts {
		uploads += attempt.Outcome.Attempts
		if attempt.Outcome.Code == "attempt-started" {
			pending++
		}
	}
	transmission := summarizeRemoteTransmission(attempts)
	completedAt := completed.Format(time.RFC3339Nano)
	if pending > 0 {
		completedAt = ""
	}
	metadata, err := jsonText(map[string]any{"requested_endpoint": plan.Endpoint, "manifest_digest": plan.Digest, "model_id": plan.Model, "prompt_version": plan.Prompt, "network_scope": "remote", "local_only": false, "cloud_transmitted": transmission.CloudTransmitted, "upload_may_have_started": transmission.UploadMayHaveStarted, "confirmed_payload_writes": transmission.ConfirmedPayloadWrites, "unknown_transmission_attempts": transmission.UnknownTransmissionAttempts, "audit_complete": pending == 0, "pending_attempts": pending, "request_attempts": uploads, "confirmed_request_attempts": uploads - pending, "completion_config": completion, "attempts": attempts})
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `insert into model_run(id,source,model_id,prompt_version,started_at,completed_at,input_count,metadata_json) values (?,?,?,?,?,?,?,?) on conflict(id) do update set completed_at=excluded.completed_at,input_count=excluded.input_count,metadata_json=excluded.metadata_json`, id, remoteSource, plan.Model, plan.Prompt, started.Format(time.RFC3339Nano), completedAt, len(attempts), metadata)
	return err
}
