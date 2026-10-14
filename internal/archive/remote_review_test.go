package archive

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRemoteReviewAuditFailureRetainsPossibleUpload(t *testing.T) {
	t.Setenv("PHOTOSCRAWL_MODEL_API_KEY", "fixture-bearer")
	f := newRemoteFixture(t)
	f.planNow(t)
	calls := 0
	client := &http.Client{Transport: syntheticRoundTripper(func(r *http.Request) (*http.Response, error) {
		calls++
		remoteFixtureSQL(t, f, func(db *sql.DB) {
			var value string
			if err := db.QueryRow(`select metadata_json from model_run where source=?`, remoteSource).Scan(&value); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(value, `"attempt-started"`) || !strings.Contains(value, `"audit_complete":false`) {
				t.Fatal("HTTP preceded durable attempt-started audit")
			}
			if _, err := db.Exec(`create trigger reject_audit before update on model_run begin select raise(abort,'synthetic-audit-refusal'); end`); err != nil {
				t.Fatal(err)
			}
		})
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(remoteEnvelope("private-rejected-sentinel", "length"))), Request: r}, nil
	})}
	_, err := Classify(context.Background(), f.paths, f.executeOptions(client))
	if err == nil || err.Error() != "remote-audit-failed" || calls != 1 {
		t.Fatalf("audit failure must stop: %v calls=%d", err, calls)
	}
	remoteFixtureSQL(t, f, func(db *sql.DB) {
		var value string
		if err := db.QueryRow(`select metadata_json from model_run where source=?`, remoteSource).Scan(&value); err != nil {
			t.Fatal(err)
		}
		var metadata map[string]any
		if err := json.Unmarshal([]byte(value), &metadata); err != nil {
			t.Fatal(err)
		}
		if metadata["upload_may_have_started"] != true || metadata["audit_complete"] != false || metadata["cloud_transmitted"] != nil {
			t.Fatalf("durable record falsely denies upload: %s", value)
		}
		if metadata["confirmed_request_attempts"] != float64(0) || metadata["pending_attempts"] != float64(1) || strings.Contains(value, `"http_status":200`) {
			t.Fatal("failed finalization claimed exact response metadata")
		}
		if strings.Contains(value, "private-rejected-sentinel") || strings.Contains(value, "raw_response") {
			t.Fatal("rejected provider text persisted")
		}
		var n int
		_ = db.QueryRow(`select count(*) from evidence_ref where source=?`, remoteSource).Scan(&n)
		if n != 0 {
			t.Fatal("failed audit escaped transaction")
		}
	})
}

func TestRemoteReviewAuditStartFailurePreventsHTTP(t *testing.T) {
	t.Setenv("PHOTOSCRAWL_MODEL_API_KEY", "fixture-bearer")
	f := newRemoteFixture(t)
	f.planNow(t)
	remoteFixtureSQL(t, f, func(db *sql.DB) {
		if _, err := db.Exec(`create trigger reject_audit_start before insert on model_run begin select raise(abort,'synthetic-start-refusal'); end`); err != nil {
			t.Fatal(err)
		}
	})
	calls := 0
	_, err := Classify(context.Background(), f.paths, f.executeOptions(fixtureRemoteClient(t, remoteEnvelope(remoteTestPayload, "stop"), 200, &calls)))
	if err == nil || err.Error() != "remote-audit-failed" || calls != 0 {
		t.Fatalf("unaudited HTTP: %v calls=%d", err, calls)
	}
	remoteFixtureSQL(t, f, func(db *sql.DB) {
		var owner string
		if err := db.QueryRow(`select claim_owner from classification_queue where asset_id=?`, f.ids[0]).Scan(&owner); err != nil {
			t.Fatal(err)
		}
		if owner != "" {
			t.Fatal("claim escaped failed audit transaction")
		}
	})
}

func TestRemoteReviewCancellationFinalizesOrRetainsPossibleUpload(t *testing.T) {
	t.Setenv("PHOTOSCRAWL_MODEL_API_KEY", "fixture-bearer")
	for _, reject := range []bool{false, true} {
		t.Run(map[bool]string{false: "finalized", true: "finalize-fails"}[reject], func(t *testing.T) {
			f := newRemoteFixture(t)
			f.planNow(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := 0
			client := &http.Client{Transport: syntheticRoundTripper(func(*http.Request) (*http.Response, error) {
				calls++
				if reject {
					remoteFixtureSQL(t, f, func(db *sql.DB) {
						if _, err := db.Exec(`create trigger reject_cancel_audit before update on model_run begin select raise(abort,'synthetic-cancel-refusal'); end`); err != nil {
							t.Fatal(err)
						}
					})
				}
				cancel()
				return nil, context.Canceled
			})}
			_, err := Classify(ctx, f.paths, f.executeOptions(client))
			want := "remote-cancelled"
			if reject {
				want = "remote-audit-failed"
			}
			if err == nil || err.Error() != want || calls != 1 {
				t.Fatalf("cancel outcome: %v calls=%d", err, calls)
			}
			remoteFixtureSQL(t, f, func(db *sql.DB) {
				var value string
				if err := db.QueryRow(`select metadata_json from model_run where source=?`, remoteSource).Scan(&value); err != nil {
					t.Fatal(err)
				}
				var metadata map[string]any
				_ = json.Unmarshal([]byte(value), &metadata)
				if metadata["audit_complete"] == reject || metadata["upload_may_have_started"] != true {
					t.Fatal("cancellation lost upload audit")
				}
				if reject && metadata["cloud_transmitted"] != nil {
					t.Fatal("failed cancel finalization claimed known transmission")
				}
				if !reject && !strings.Contains(value, `"code":"cancelled"`) {
					t.Fatal("cancel not finalized")
				}
			})
		})
	}
}

func TestRemoteReviewNonceDriftBeforeExecution(t *testing.T) {
	t.Setenv("PHOTOSCRAWL_MODEL_API_KEY", "fixture-bearer")
	for _, kind := range []string{"owner", "expiry", "active"} {
		t.Run(kind, func(t *testing.T) {
			f := newRemoteFixture(t)
			remoteFixtureSQL(t, f, func(db *sql.DB) {
				if _, err := db.Exec(`update classification_queue set claim_owner='expired-A',claim_expires_at='2026-10-13T10:00:00Z' where asset_id=?`, f.ids[0]); err != nil {
					t.Fatal(err)
				}
			})
			f.planNow(t)
			remoteFixtureSQL(t, f, func(db *sql.DB) {
				queries := map[string]string{"owner": `update classification_queue set claim_owner='expired-B' where asset_id=?`, "expiry": `update classification_queue set claim_expires_at='2026-10-13T11:00:00Z' where asset_id=?`, "active": `update classification_queue set claim_owner='foreign-active',claim_expires_at='2026-10-15T10:00:00Z' where asset_id=?`}
				if _, err := db.Exec(queries[kind], f.ids[0]); err != nil {
					t.Fatal(err)
				}
			})
			calls := 0
			_, err := Classify(context.Background(), f.paths, f.executeOptions(fixtureRemoteClient(t, remoteEnvelope(remoteTestPayload, "stop"), 200, &calls)))
			if err == nil || calls != 0 {
				t.Fatalf("changed planned nonce uploaded: %v calls=%d", err, calls)
			}
			remoteFixtureSQL(t, f, func(db *sql.DB) {
				var n int
				_ = db.QueryRow(`select count(*) from evidence_ref where source=?`, remoteSource).Scan(&n)
				if n != 0 {
					t.Fatal("unsafe success evidence")
				}
				if kind == "active" {
					var owner string
					_ = db.QueryRow(`select claim_owner from classification_queue where asset_id=?`, f.ids[0]).Scan(&owner)
					if owner != "foreign-active" {
						t.Fatal("foreign active claim stolen")
					}
				}
			})
		})
	}
}

func TestRemoteReviewUnsafeIntermediateBeforePublication(t *testing.T) {
	root := t.TempDir()
	intermediate := filepath.Join(root, "writable")
	private := filepath.Join(intermediate, "private")
	if err := os.MkdirAll(private, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(intermediate, 0777); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(private, "plan.json")
	if err := writePrivateRemoteFile(path, []byte("synthetic")); err == nil {
		t.Fatal("unsafe intermediate allowed manifest publication")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("unsafe path published a manifest")
	}
}

func TestRemoteReviewNonceFenceBetweenValidationAndClaim(t *testing.T) {
	t.Setenv("PHOTOSCRAWL_MODEL_API_KEY", "fixture-bearer")
	for _, kind := range []string{"owner", "expiry", "null-vs-empty", "active"} {
		t.Run(kind, func(t *testing.T) {
			f := newRemoteFixture(t)
			if kind != "null-vs-empty" {
				remoteFixtureSQL(t, f, func(db *sql.DB) {
					if _, err := db.Exec(`update classification_queue set claim_owner='expired-A',claim_expires_at='2026-10-13T10:00:00Z' where asset_id=?`, f.ids[0]); err != nil {
						t.Fatal(err)
					}
				})
			}
			plan := f.planNow(t)
			entry := plan.Entries[0]
			var observed remoteSelection
			remoteFixtureSQL(t, f, func(db *sql.DB) {
				tx, err := db.Begin()
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				observed, _, err = validateRemoteEntry(context.Background(), tx, entry, plan, f.options.Now())
				if err != nil {
					t.Fatal(err)
				}
				if err = tx.Commit(); err != nil {
					t.Fatal(err)
				}
			})
			remoteFixtureSQL(t, f, func(db *sql.DB) {
				queries := map[string]string{"owner": `update classification_queue set claim_owner='expired-B' where asset_id=?`, "expiry": `update classification_queue set claim_expires_at='2026-10-13T11:00:00Z' where asset_id=?`, "null-vs-empty": `update classification_queue set claim_expires_at='' where asset_id=?`, "active": `update classification_queue set claim_owner='foreign-active',claim_expires_at='2026-10-15T10:00:00Z' where asset_id=?`}
				if _, err := db.Exec(queries[kind], f.ids[0]); err != nil {
					t.Fatal(err)
				}
			})
			remoteFixtureSQL(t, f, func(db *sql.DB) {
				tx, err := db.Begin()
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				if err = claimRemoteInput(context.Background(), tx, &observed, entry, "new-owned-claim", f.options.Now()); err == nil {
					t.Fatal("nonce predicate stole changed claim")
				}
			})
			calls := 0
			if _, err := Classify(context.Background(), f.paths, f.executeOptions(fixtureRemoteClient(t, remoteEnvelope(remoteTestPayload, "stop"), 200, &calls))); err == nil || calls != 0 {
				t.Fatalf("nonce drift reached HTTP: %v", err)
			}
		})
	}
}

func TestRemoteReviewManifestRequiresNonceFieldsAndVersion(t *testing.T) {
	for _, field := range []string{"planned_claim_owner", "planned_claim_expires_at", "version"} {
		t.Run(field, func(t *testing.T) {
			f := newRemoteFixture(t)
			plan := f.planNow(t)
			if plan.Version != 2 {
				t.Fatal("nonce wire contract version not advanced")
			}
			var data []byte
			if field == "version" {
				plan.Version = 1
				plan.Digest = plan.digest()
				data, _ = json.Marshal(plan)
			} else {
				data, _ = os.ReadFile(f.plan)
				var raw map[string]any
				_ = json.Unmarshal(data, &raw)
				entries := raw["entries"].([]any)
				delete(entries[0].(map[string]any), field)
				data, _ = json.Marshal(raw)
			}
			if err := os.WriteFile(f.plan, data, 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := readRemotePlan(f.plan, time.Date(2026, 10, 14, 11, 0, 0, 0, time.UTC)); err == nil {
				t.Fatal("old or missing nonce contract accepted")
			}
		})
	}
}
