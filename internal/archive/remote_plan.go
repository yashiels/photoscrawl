package archive

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"time"
	"unicode/utf8"
)

type remotePlan struct {
	Version  int               `json:"version"`
	Archive  string            `json:"archive"`
	Created  time.Time         `json:"created_at"`
	Expires  time.Time         `json:"expires_at"`
	Model    string            `json:"model"`
	Endpoint string            `json:"endpoint"`
	Prompt   string            `json:"prompt_version"`
	Retry    bool              `json:"retry_content_failed"`
	Entries  []remotePlanEntry `json:"entries"`
	Digest   string            `json:"digest"`
}

type remotePlanEntry struct {
	Asset          string  `json:"asset_id"`
	Source         string  `json:"source_library_id"`
	SourceIdentity string  `json:"source_identity"`
	Fingerprint    string  `json:"input_fingerprint"`
	State          string  `json:"planned_state"`
	Updated        string  `json:"queue_updated_at"`
	ClaimOwner     string  `json:"planned_claim_owner"`
	ClaimExpiresAt *string `json:"planned_claim_expires_at"`
	Disposition    string  `json:"disposition"`
	Reason         string  `json:"reason"`
	Resource       string  `json:"resource_id"`
	Kind           string  `json:"resource_kind"`
	Designation    string  `json:"resource_designation"`
	SHA256         string  `json:"resource_sha256"`
	Bytes          int64   `json:"resource_bytes"`
}

type remoteSelection struct {
	input          classifyInput
	state          string
	updated        string
	lease          sql.NullString
	sourceIdentity string
}

func strictRemoteJSON(data []byte, target any) error {
	if !utf8.Valid(data) || !uniqueRemoteJSON(data) {
		return errors.New("invalid-private-json")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if decoder.Decode(target) != nil || decoder.Decode(new(any)) != io.EOF {
		return errors.New("invalid-private-json")
	}
	return nil
}

func uniqueRemoteJSON(data []byte) bool {
	decoder := json.NewDecoder(bytes.NewReader(data))
	var visit func(int) bool
	visit = func(depth int) bool {
		if depth > 64 {
			return false
		}
		token, err := decoder.Token()
		if err != nil {
			return false
		}
		delim, ok := token.(json.Delim)
		if !ok {
			return true
		}
		switch delim {
		case '{':
			keys := map[string]bool{}
			for decoder.More() {
				key, err := decoder.Token()
				if err != nil {
					return false
				}
				text, ok := key.(string)
				if !ok || keys[text] {
					return false
				}
				keys[text] = true
				if !visit(depth + 1) {
					return false
				}
			}
		case '[':
			for decoder.More() {
				if !visit(depth + 1) {
					return false
				}
			}
		default:
			return false
		}
		_, err = decoder.Token()
		return err == nil
	}
	if !visit(0) {
		return false
	}
	_, err := decoder.Token()
	return err == io.EOF
}

func remoteDigest(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }

func (p remotePlan) digest() string {
	p.Digest = ""
	data, _ := json.Marshal(p)
	return remoteDigest(data)
}

func readRemotePlan(path string, now time.Time) (remotePlan, error) {
	var plan remotePlan
	data, err := readPrivateRemoteFile(path)
	if err != nil {
		return plan, err
	}
	if strictRemoteJSON(data, &plan) != nil {
		return plan, errors.New("invalid-remote-plan")
	}
	endpoint, err := normalizeRemoteURL(plan.Endpoint)
	if err != nil || endpoint != plan.Endpoint || !validRemoteModel(plan.Model) || plan.Version != 2 || plan.Prompt != remotePromptVersion || plan.Archive == "" || plan.Digest != plan.digest() || !plan.Expires.Equal(plan.Created.Add(24*time.Hour)) || now.Before(plan.Created) || !now.Before(plan.Expires) || len(plan.Entries) == 0 || len(plan.Entries) > remoteAssetLimit {
		return plan, errors.New("invalid-remote-plan")
	}
	var raw struct {
		Entries []map[string]json.RawMessage `json:"entries"`
	}
	if json.Unmarshal(data, &raw) != nil {
		return plan, errors.New("invalid-remote-plan")
	}
	seen := map[string]bool{}
	for i, entry := range plan.Entries {
		fields := raw.Entries[i]
		if len(fields) != 15 {
			return plan, errors.New("invalid-remote-plan")
		}
		for name, value := range fields {
			if name == "planned_claim_expires_at" {
				if string(value) != "null" && (len(value) == 0 || value[0] != '"') {
					return plan, errors.New("invalid-remote-plan")
				}
				continue
			}
			if name == "resource_bytes" {
				if string(value) == "null" {
					return plan, errors.New("invalid-remote-plan")
				}
				continue
			}
			if len(value) == 0 || value[0] != '"' {
				return plan, errors.New("invalid-remote-plan")
			}
		}
		if entry.Asset == "" || seen[entry.Asset] || entry.Source == "" || entry.SourceIdentity == "" || entry.Fingerprint == "" {
			return plan, errors.New("invalid-remote-plan")
		}
		seen[entry.Asset] = true
		switch entry.Disposition {
		case "planned-upload":
			if entry.Reason != "eligible" || entry.Resource == "" || entry.Kind == "" || entry.Designation == "" || len(entry.SHA256) != 64 || entry.Bytes <= 0 || entry.Bytes > remoteResourceLimit {
				return plan, errors.New("invalid-remote-plan")
			}
		case "skipped", "unavailable":
			switch entry.Reason {
			case "ineligible", "no-local-image", "resource-unavailable", "resource-oversize":
			default:
				return plan, errors.New("invalid-remote-plan")
			}
		default:
			return plan, errors.New("invalid-remote-plan")
		}
	}
	return plan, nil
}

func loadRemoteSelection(ctx context.Context, tx *sql.Tx, id string) (remoteSelection, error) {
	var s remoteSelection
	var queue, owner, libraryPath string
	var lease sql.NullString
	err := tx.QueryRowContext(ctx, `select q.id,q.claim_owner,q.state,q.updated_at,q.claim_expires_at,s.library_path
from classification_queue q join asset a on a.id=q.asset_id join source_library s on s.id=q.source_library_id
where q.asset_id=? and a.deleted_at is null`, id).Scan(&queue, &owner, &s.state, &s.updated, &lease, &libraryPath)
	if err != nil {
		return s, errors.New("selection-unavailable")
	}
	input, err := loadClaimedClassifyInput(ctx, tx, queue, owner)
	if err != nil {
		return s, errors.New("selection-unavailable")
	}
	s.input = *input
	s.lease = lease
	s.sourceIdentity = stableID("remote_source", input.SourceLibraryID, libraryPath)
	return s, nil
}

func (s remoteSelection) eligible(now time.Time, retry bool) bool {
	if s.input.ClaimOwner != "" {
		expires, err := time.Parse(time.RFC3339Nano, s.lease.String)
		if err != nil || expires.After(now) {
			return false
		}
	}
	switch s.state {
	case "pending", "metadata_classified":
		return true
	case "content_failed":
		updated, err := time.Parse(time.RFC3339Nano, s.updated)
		return retry || err == nil && !updated.After(now.Add(-failedContentRetryCooldown))
	}
	return false
}

func remoteImage(input classifyInput) (classifyResource, bool) {
	if input.MediaType != "image" {
		return classifyResource{}, false
	}
	for _, resource := range input.Resources {
		if resource.AvailableLocally && !resource.NeedsDownload && resource.LocalPath != "" && classifiableImagePath(resource.LocalPath) {
			return resource, true
		}
	}
	return classifyResource{}, false
}

func readRemoteResource(path string) ([]byte, error) {
	f, err := openRemoteResource(path)
	if err != nil {
		return nil, errors.New("resource-unavailable")
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 {
		return nil, errors.New("resource-unavailable")
	}
	if info.Size() > remoteResourceLimit {
		return nil, errors.New("resource-oversize")
	}
	return readRemoteResourceBytes(f, info.Size())
}

func readRemoteResourceBytes(reader io.Reader, size int64) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(reader, remoteResourceLimit+1))
	if err != nil {
		return nil, errors.New("resource-unavailable")
	}
	if len(data) > remoteResourceLimit {
		return nil, errors.New("resource-oversize")
	}
	if int64(len(data)) != size {
		return nil, errors.New("resource-unavailable")
	}
	return data, nil
}

func planRemote(ctx context.Context, paths Paths, opts ClassifyOptions, now time.Time) (ClassifyResult, error) {
	result := ClassifyResult{Classifier: remoteSource, Remote: &RemoteClassifyResult{remoteTransmission: summarizeRemoteTransmission(nil), Status: "planned", NetworkScope: "remote"}}
	o := opts.RemoteModel
	data, err := readPrivateRemoteFile(o.AssetIDsFile)
	if err != nil {
		return result, err
	}
	var ids []string
	if strictRemoteJSON(data, &ids) != nil || len(ids) == 0 || len(ids) > remoteAssetLimit {
		return result, errors.New("invalid-asset-selection")
	}
	seen := map[string]bool{}
	for _, id := range ids {
		if id == "" || seen[id] {
			return result, errors.New("invalid-asset-selection")
		}
		seen[id] = true
	}
	db, err := openArchiveReadOnly(ctx, paths.Database)
	if err != nil {
		return result, errors.New("archive-unavailable")
	}
	defer db.Close()
	identity, err := remoteArchiveFileIdentity(paths.Database)
	if err != nil {
		return result, err
	}
	endpoint, _ := normalizeRemoteURL(o.URL)
	plan := remotePlan{Version: 2, Archive: identity, Created: now, Expires: now.Add(24 * time.Hour), Model: o.Model, Endpoint: endpoint, Prompt: remotePromptVersion, Retry: o.RetryContentFailed}
	err = db.WithTx(ctx, func(tx *sql.Tx) error {
		for _, id := range ids {
			s, err := loadRemoteSelection(ctx, tx, id)
			if err != nil {
				return err
			}
			e := remotePlanEntry{Asset: id, Source: s.input.SourceLibraryID, SourceIdentity: s.sourceIdentity, Fingerprint: s.input.InputFingerprint, State: s.state, Updated: s.updated, ClaimOwner: s.input.ClaimOwner, Disposition: "skipped", Reason: "ineligible"}
			if s.lease.Valid {
				expiry := s.lease.String
				e.ClaimExpiresAt = &expiry
			}
			if s.eligible(now, plan.Retry) {
				resource, ok := remoteImage(s.input)
				if !ok {
					e.Disposition = "unavailable"
					e.Reason = "no-local-image"
				} else {
					data, err := readRemoteResource(resource.LocalPath)
					if err != nil {
						e.Disposition = "unavailable"
						e.Reason = err.Error()
						if e.Reason == "resource-oversize" {
							e.Disposition = "skipped"
						}
					} else {
						e.Disposition = "planned-upload"
						e.Reason = "eligible"
						e.Resource = resource.ID
						e.Kind = resource.ResourceType
						e.Designation = s.input.localPathClass(resource.LocalPath)
						e.SHA256 = remoteDigest(data)
						e.Bytes = int64(len(data))
					}
				}
			}
			plan.Entries = append(plan.Entries, e)
			switch e.Disposition {
			case "planned-upload":
				result.Remote.Planned++
			case "unavailable":
				result.Remote.Unavailable++
			default:
				result.Remote.Skipped++
			}
		}
		return nil
	})
	if err != nil {
		return result, errors.New("selection-unavailable")
	}
	plan.Digest = plan.digest()
	encoded, err := json.Marshal(plan)
	if err != nil {
		return result, errors.New("plan-encoding-failed")
	}
	if err = writePrivateRemoteFile(o.PlanOut, encoded); err != nil {
		return result, err
	}
	return result, nil
}

func validateRemoteEntry(ctx context.Context, tx *sql.Tx, entry remotePlanEntry, plan remotePlan, now time.Time) (remoteSelection, []byte, error) {
	s, err := loadRemoteSelection(ctx, tx, entry.Asset)
	if err != nil {
		return s, nil, err
	}
	if s.input.SourceLibraryID != entry.Source || s.sourceIdentity != entry.SourceIdentity || s.input.InputFingerprint != entry.Fingerprint {
		return s, nil, errors.New("selection-changed")
	}
	if !s.matchesPlannedClaim(entry) {
		return s, nil, errors.New("claim-snapshot-changed")
	}
	if s.state == "content_classified" {
		return s, nil, errors.New("already-classified")
	}
	if s.state != entry.State || s.updated != entry.Updated || !s.eligible(now, plan.Retry) {
		return s, nil, errors.New("eligibility-changed")
	}
	resource, ok := remoteImage(s.input)
	if !ok || resource.ID != entry.Resource || resource.ResourceType != entry.Kind || s.input.localPathClass(resource.LocalPath) != entry.Designation {
		return s, nil, errors.New("resource-changed")
	}
	data, err := readRemoteResource(resource.LocalPath)
	if err != nil {
		return s, nil, err
	}
	if int64(len(data)) != entry.Bytes || remoteDigest(data) != entry.SHA256 {
		return s, nil, errors.New("resource-changed")
	}
	return s, data, nil
}

func (s remoteSelection) matchesPlannedClaim(entry remotePlanEntry) bool {
	if s.input.ClaimOwner != entry.ClaimOwner || s.lease.Valid != (entry.ClaimExpiresAt != nil) {
		return false
	}
	return !s.lease.Valid || s.lease.String == *entry.ClaimExpiresAt
}
