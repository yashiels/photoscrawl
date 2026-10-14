package archive

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/openclaw/photoscrawl/internal/photos"
)

type remoteFixture struct {
	paths   Paths
	options ClassifyOptions
	ids     []string
	image   string
	plan    string
}

func newRemoteFixture(t *testing.T) remoteFixture {
	t.Helper()
	root := t.TempDir()
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	paths := Paths{DataDir: root, Database: filepath.Join(root, "photos.sqlite")}
	image := filepath.Join(root, "synthetic.jpg")
	if err := os.WriteFile(image, []byte("synthetic image bytes"), 0600); err != nil {
		t.Fatal(err)
	}
	library := filepath.Join(root, "Synthetic.photoslibrary")
	if err := mkdirLibrary(library); err != nil {
		t.Fatal(err)
	}
	provider := fakeProvider{snapshot: photos.LibrarySnapshot{Provider: "fake", Assets: []photos.Asset{
		{LocalIdentifier: "synthetic-selected", MediaType: "image", Resources: []photos.Resource{{SourceIdentifier: "synthetic-resource", Type: "photo", LocalPath: image, AvailableLocally: true, Availability: "local"}}},
		{LocalIdentifier: "synthetic-unavailable", MediaType: "image"},
		{LocalIdentifier: "synthetic-unrelated", MediaType: "image", Resources: []photos.Resource{{SourceIdentifier: "unrelated-resource", Type: "photo", LocalPath: image, AvailableLocally: true, Availability: "local"}}},
	}}}
	if _, err := Crawl(context.Background(), paths, CrawlOptions{LibraryPath: library, Provider: provider, Now: fixedClock("2026-10-14T10:00:00Z")}); err != nil {
		t.Fatal(err)
	}
	db, err := openArchiveReadOnly(context.Background(), paths.Database)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ids := []string{}
	for _, local := range []string{"synthetic-selected", "synthetic-unavailable"} {
		var id string
		if err := db.DB().QueryRow(`select id from asset where local_identifier=?`, local).Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	idsFile := filepath.Join(root, "ids.json")
	data, _ := json.Marshal(ids)
	if err := os.WriteFile(idsFile, data, 0600); err != nil {
		t.Fatal(err)
	}
	plan := filepath.Join(root, "consent.json")
	return remoteFixture{paths: paths, ids: ids, image: image, plan: plan, options: ClassifyOptions{Now: fixedClock("2026-10-14T11:00:00Z"), RemoteModel: RemoteModelOptions{Model: "arbitrary/provider:Exact-ID", URL: "https://example.test/v1", DryRun: true, AssetIDsFile: idsFile, PlanOut: plan}}}
}

func (f remoteFixture) planNow(t *testing.T) remotePlan {
	t.Helper()
	result, err := Classify(context.Background(), f.paths, f.options)
	if err != nil {
		t.Fatal(err)
	}
	if result.Remote.Planned != 1 || result.Remote.Unavailable != 1 {
		t.Fatalf("plan counts: %+v", result.Remote)
	}
	encoded, _ := json.Marshal(result)
	for _, private := range []string{f.ids[0], f.image, f.plan, f.options.RemoteModel.Model, f.options.RemoteModel.URL} {
		if strings.Contains(string(encoded), private) {
			t.Fatal("private plan data in result")
		}
	}
	plan, err := readRemotePlan(f.plan, f.options.Now())
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func (f remoteFixture) executeOptions(client *http.Client) ClassifyOptions {
	return ClassifyOptions{Now: f.options.Now, Limit: 1000, RemoteModel: RemoteModelOptions{Plan: f.plan, AllowUpload: true, client: client}}
}

func remoteFixtureSQL(t *testing.T, f remoteFixture, fn func(*sql.DB)) {
	t.Helper()
	db, err := openArchiveStore(context.Background(), f.paths.Database)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	fn(db.DB())
}

func remoteEnvelope(content, finish string) string {
	data, _ := json.Marshal(map[string]any{"choices": []any{map[string]any{"finish_reason": finish, "message": map[string]any{"content": content}}}, "usage": map[string]int{"prompt_tokens": 5, "completion_tokens": 10, "total_tokens": 15}})
	return string(data)
}

func fixtureRemoteClient(t *testing.T, body string, status int, calls *int) *http.Client {
	t.Helper()
	return &http.Client{Transport: syntheticRoundTripper(func(r *http.Request) (*http.Response, error) {
		*calls++
		if r.Header.Get("Authorization") != "Bearer fixture-bearer" {
			t.Error("missing bearer")
		}
		return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
	})}
}

func TestRemoteManifestExecutionFreezesSelection(t *testing.T) {
	t.Setenv("PHOTOSCRAWL_MODEL_API_KEY", "fixture-bearer")
	f := newRemoteFixture(t)
	before := []string{}
	remoteFixtureSQL(t, f, func(db *sql.DB) {
		rows, err := db.Query(`select state||':'||updated_at||':'||claim_owner from classification_queue order by id`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		for rows.Next() {
			var s string
			_ = rows.Scan(&s)
			before = append(before, s)
		}
	})
	plan := f.planNow(t)
	remoteFixtureSQL(t, f, func(db *sql.DB) {
		rows, err := db.Query(`select state||':'||updated_at||':'||claim_owner from classification_queue order by id`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		after := []string{}
		for rows.Next() {
			var s string
			_ = rows.Scan(&s)
			after = append(after, s)
		}
		if fmt.Sprint(before) != fmt.Sprint(after) {
			t.Fatal("planning mutated queue")
		}
	})
	remoteFixtureSQL(t, f, func(db *sql.DB) {
		_, err := db.Exec(`insert into asset_resource(id,asset_id,source_identifier,resource_type,uti,original_filename,local_path,file_size,sha256,available_locally,needs_download) select 'new-resource',?,'new','photo','','synthetic.jpg',local_path,0,'',1,0 from asset_resource limit 1`, f.ids[1])
		if err != nil {
			t.Fatal(err)
		}
	})
	calls := 0
	client := fixtureRemoteClient(t, remoteEnvelope(remoteTestPayload, "stop"), 200, &calls)
	result, err := Classify(context.Background(), f.paths, f.executeOptions(client))
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 || result.ContentClassified != 1 || result.Remote.Attempts != 1 || result.Remote.CloudTransmitted != nil {
		t.Fatalf("execution counts: %+v calls=%d", result, calls)
	}
	remoteFixtureSQL(t, f, func(db *sql.DB) {
		var evidence string
		if err := db.QueryRow(`select value_json from evidence_ref where source=?`, remoteSource).Scan(&evidence); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(evidence, `"raw_response"`) || !strings.Contains(evidence, plan.Entries[0].SHA256) {
			t.Fatal("missing private success evidence")
		}
		var state string
		_ = db.QueryRow(`select state from classification_queue where asset_id=?`, f.ids[1]).Scan(&state)
		if state != "pending" {
			t.Fatal("unavailable entry promoted")
		}
	})
	result, err = Classify(context.Background(), f.paths, f.executeOptions(client))
	if err != nil || calls != 1 || result.Remote.Skipped != 1 || result.Remote.CloudTransmitted == nil || *result.Remote.CloudTransmitted || result.Remote.ConfirmedPayloadWrites != 0 || result.Remote.UnknownTransmissionAttempts != 0 || result.Remote.UploadMayHaveStarted {
		t.Fatalf("classified replay must skip: %v %+v", err, result)
	}
}

func TestRemoteUnsuccessfulResponsesNeverPersistContent(t *testing.T) {
	const sentinel = "sentinel-private-bearer-image-ocr-location"
	badPayload := strings.Replace(remoteTestPayload, "A synthetic scene.", sentinel, 1)
	cases := map[string]string{
		"empty":            remoteEnvelope("", "stop"),
		"no-choice":        `{"choices":[],"private":"` + sentinel + `"}`,
		"provider-error":   `{"error":{"message":"` + sentinel + `"},"choices":[]}`,
		"length":           remoteEnvelope(badPayload, "length"),
		"content-filter":   remoteEnvelope(badPayload, "content_filter"),
		"unknown-finish":   remoteEnvelope(badPayload, sentinel),
		"malformed-object": remoteEnvelope(sentinel, "stop"),
		"missing-key":      remoteEnvelope(`{"scene_summary":"`+sentinel+`"}`, "stop"),
		"type-mismatch":    remoteEnvelope(strings.Replace(badPayload, `"cluster_terms":[]`, `"cluster_terms":[42]`, 1), "stop"),
		"unknown-key":      remoteEnvelope(strings.Replace(badPayload, `"cluster_terms":[]`, `"cluster_terms":[],"extra":true`, 1), "stop"),
		"prompt-leakage":   remoteEnvelope(strings.Replace(badPayload, sentinel, "KEYS: "+sentinel, 1), "stop"),
		"trailing":         remoteEnvelope(badPayload+sentinel, "stop"),
		"oversize":         strings.Repeat("x", remoteResponseLimit) + sentinel,
		"refusal":          `{"choices":[{"finish_reason":"stop","message":{"content":"` + sentinel + `","refusal":"` + sentinel + `"}}]}`,
		"two-choices":      `{"choices":[{},{}],"private":"` + sentinel + `"}`,
		"invalid-json":     `{"private":"` + sentinel + `"`,
	}
	for _, key := range []string{"scene_summary", "visible_text_summary", "people_presence", "place_candidates", "landmark_candidates", "merchant_or_venue_candidates", "food_or_objects", "privacy_sensitivity", "cluster_terms", "uncertainties"} {
		for i, fragment := range []string{"return only valid compact", "Do Not Use Markdown Fences", "use candidates, not truth", "KEYS:"} {
			var payload map[string]any
			_ = json.Unmarshal([]byte(remoteTestPayload), &payload)
			text := fragment + " " + sentinel
			if key == "scene_summary" || key == "visible_text_summary" || key == "people_presence" {
				payload[key] = text
			} else {
				payload[key] = []string{text}
			}
			data, _ := json.Marshal(payload)
			cases[fmt.Sprintf("policy-%s-%d", key, i)] = remoteEnvelope(string(data), "stop")
		}
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			t.Setenv("PHOTOSCRAWL_MODEL_API_KEY", "fixture-bearer")
			f := newRemoteFixture(t)
			f.planNow(t)
			calls := 0
			result, err := Classify(context.Background(), f.paths, f.executeOptions(fixtureRemoteClient(t, body, 200, &calls)))
			if err != nil || calls != 1 || result.ContentClassificationFailures != 1 {
				t.Fatalf("failed response counts: %v %+v calls=%d", err, result, calls)
			}
			encoded, _ := json.Marshal(result)
			if strings.Contains(string(encoded), sentinel) {
				t.Fatal("CLI leakage")
			}
			remoteFixtureSQL(t, f, func(db *sql.DB) {
				for _, query := range []string{`select reason from classification_queue`, `select value_json from evidence_ref`, `select metadata_json from model_run`} {
					rows, err := db.Query(query)
					if err != nil {
						t.Fatal(err)
					}
					for rows.Next() {
						var value string
						if err := rows.Scan(&value); err != nil {
							t.Fatal(err)
						}
						if strings.Contains(value, sentinel) || strings.Contains(value, `"raw_response"`) {
							t.Fatal("unsuccessful private text persisted")
						}
					}
					rows.Close()
				}
				var n int
				_ = db.QueryRow(`select count(*) from model_observation`).Scan(&n)
				if n != 0 {
					t.Fatal("unsuccessful observations persisted")
				}
			})
		})
	}
}

func TestRemoteEvidenceNamespacesAndAttemptHistory(t *testing.T) {
	t.Setenv("PHOTOSCRAWL_MODEL_API_KEY", "fixture-bearer")
	f := newRemoteFixture(t)
	remoteFixtureSQL(t, f, func(db *sql.DB) {
		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		s, err := loadRemoteSelection(context.Background(), tx, f.ids[0])
		if err != nil {
			t.Fatal(err)
		}
		classifier := localModelClassifier{modelID: f.options.RemoteModel.Model, promptVersion: localModelPromptVersion}
		if _, err = writeLocalModelClassification(context.Background(), tx, s.input, classifier, localModelResult{RawResponse: remoteTestPayload, Payload: map[string]any{"scene_summary": "Local synthetic scene"}, Observations: []contentObservation{{ObservationType: "scene_summary", ValueText: "Local synthetic scene", Value: map[string]any{"text": "Local synthetic scene"}}}}, f.options.Now()); err != nil {
			t.Fatal(err)
		}
		if err = tx.Commit(); err != nil {
			t.Fatal(err)
		}
	})
	calls := 0
	client := fixtureRemoteClient(t, remoteEnvelope(remoteTestPayload, "stop"), 200, &calls)
	for i, endpoint := range []string{"https://one.test", "https://one.test", "https://two.test", "https://one.test"} {
		remoteFixtureSQL(t, f, func(db *sql.DB) {
			if _, err := db.Exec(`update classification_queue set state='pending',updated_at='2026-10-14T10:00:00Z' where asset_id=?`, f.ids[0]); err != nil {
				t.Fatal(err)
			}
		})
		if i == 3 {
			if err := os.WriteFile(f.image, []byte("new synthetic resource"), 0600); err != nil {
				t.Fatal(err)
			}
		}
		f.options.RemoteModel.URL = endpoint
		f.plan = filepath.Join(filepath.Dir(f.plan), fmt.Sprintf("consent-%d.json", i))
		f.options.RemoteModel.PlanOut = f.plan
		f.planNow(t)
		if _, err := Classify(context.Background(), f.paths, f.executeOptions(client)); err != nil {
			t.Fatal(err)
		}
	}
	remoteFixtureSQL(t, f, func(db *sql.DB) {
		for query, want := range map[string]int{
			`select count(*) from model_run where source='remote_multimodal'`:                                                                        4,
			`select count(*) from evidence_ref where source='remote_multimodal'`:                                                                     3,
			`select count(*) from evidence_ref where source='local_multimodal'`:                                                                      1,
			`select count(*) from model_observation where source='local_multimodal'`:                                                                 1,
			`select count(*) from model_observation where source='remote_multimodal'`:                                                                6,
			`select count(*) from observation_fts_rowid f join model_observation m on m.id=f.observation_id where m.source='remote_multimodal'`:      6,
			`select count(distinct e.pointer) from model_observation m join evidence_ref e on e.id=m.evidence_id where m.source='remote_multimodal'`: 3,
			`select count(distinct json_extract(e.value_json,'$.resource.resource_sha256')) from model_observation m join evidence_ref e on e.id=m.evidence_id where m.source='remote_multimodal' and json_extract(e.value_json,'$.requested_endpoint')='https://one.test/v1/chat/completions'`: 2,
		} {
			var n int
			if err := db.QueryRow(query).Scan(&n); err != nil {
				t.Fatal(err)
			}
			if n != want {
				t.Fatalf("history count %d want %d: %s", n, want, query)
			}
		}
	})
}

func TestRemoteDriftRefusedBeforeAnyUpload(t *testing.T) {
	for _, kind := range []string{"fingerprint", "state", "resource", "hash", "deleted", "source", "expired", "tampered", "archive"} {
		t.Run(kind, func(t *testing.T) {
			t.Setenv("PHOTOSCRAWL_MODEL_API_KEY", "fixture-bearer")
			f := newRemoteFixture(t)
			f.planNow(t)
			switch kind {
			case "fingerprint", "state", "deleted", "source", "resource":
				remoteFixtureSQL(t, f, func(db *sql.DB) {
					queries := map[string]string{"fingerprint": `update classification_queue set input_fingerprint='changed' where asset_id=?`, "state": `update classification_queue set state='content_failed' where asset_id=?`, "deleted": `update asset set deleted_at='deleted' where id=?`, "source": `update source_library set library_path='changed' where id in (select source_library_id from asset where id=?)`, "resource": `update asset_resource set resource_type='changed' where asset_id=?`}
					if _, err := db.Exec(queries[kind], f.ids[0]); err != nil {
						t.Fatal(err)
					}
				})
			case "hash":
				if err := os.WriteFile(f.image, []byte("changed synthetic bytes"), 0600); err != nil {
					t.Fatal(err)
				}
			case "expired":
				f.options.Now = func() time.Time { return time.Date(2026, 10, 16, 11, 0, 0, 0, time.UTC) }
			case "tampered":
				data, _ := os.ReadFile(f.plan)
				data = []byte(strings.Replace(string(data), "Exact-ID", "tampered", 1))
				if err := os.WriteFile(f.plan, data, 0600); err != nil {
					t.Fatal(err)
				}
			case "archive":
				f.paths.Database = filepath.Join(t.TempDir(), "missing.sqlite")
			}
			calls := 0
			_, err := Classify(context.Background(), f.paths, f.executeOptions(fixtureRemoteClient(t, remoteEnvelope(remoteTestPayload, "stop"), 200, &calls)))
			if err == nil || calls != 0 {
				t.Fatalf("drift uploaded: %v calls=%d", err, calls)
			}
		})
	}
}

func TestRemoteRetryOneAttempt(t *testing.T) {
	t.Setenv("PHOTOSCRAWL_MODEL_API_KEY", "fixture-bearer")
	for _, retry := range []bool{false, true} {
		t.Run(fmt.Sprint(retry), func(t *testing.T) {
			f := newRemoteFixture(t)
			remoteFixtureSQL(t, f, func(db *sql.DB) {
				_, err := db.Exec(`update classification_queue set state='content_failed',updated_at='2026-10-14T10:00:00Z' where asset_id=?`, f.ids[0])
				if err != nil {
					t.Fatal(err)
				}
			})
			f.options.RemoteModel.RetryContentFailed = retry
			result, err := Classify(context.Background(), f.paths, f.options)
			if err != nil {
				t.Fatal(err)
			}
			if (result.Remote.Planned == 1) != retry {
				t.Fatal("retry policy ignored")
			}
			calls := 0
			result, err = Classify(context.Background(), f.paths, f.executeOptions(fixtureRemoteClient(t, `{"error":{"message":"secret"}}`, 422, &calls)))
			if err != nil || calls != result.Remote.Attempts || calls > 1 || (calls == 1) != retry {
				t.Fatalf("retry repeat: %v %d", err, calls)
			}
		})
	}
}

func TestRemoteTLSBearerRedirectProxyAndFailurePrivacy(t *testing.T) {
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:1")
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:1")
	calls := 0
	redirectCalls := 0
	target := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { redirectCalls++ }))
	defer target.Close()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Header.Get("Authorization") != "Bearer fixture-bearer" || r.URL.Path != "/v1/chat/completions" {
			t.Error("wrong bearer or endpoint")
		}
		data, _ := io.ReadAll(r.Body)
		var request openAIChatCompletionRequest
		_ = json.Unmarshal(data, &request)
		if request.Model != "arbitrary/Exact-ID" || request.MaxTokens != 2048 {
			t.Error("model/budget changed")
		}
		if calls == 1 {
			io.WriteString(w, remoteEnvelope(remoteTestPayload, "stop"))
			return
		}
		w.Header().Set("Location", target.URL)
		w.WriteHeader(307)
		io.WriteString(w, "fixture-bearer data:image/jpeg;base64,PRIVATE OCR LOCATION")
	}))
	defer server.Close()
	client := newRemoteClient()
	defer client.CloseIdleConnections()
	client.Transport.(*http.Transport).TLSClientConfig = server.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
	endpoint, _ := normalizeRemoteURL(server.URL)
	body, err := remoteRequest("arbitrary/Exact-ID", []byte("synthetic"), CompletionConfig{MaxTokens: 2048})
	if err != nil {
		t.Fatal(err)
	}
	out := callRemote(context.Background(), client, endpoint, "fixture-bearer", body)
	if out.success == nil || out.Attempts != 1 {
		t.Fatalf("TLS request failed: %+v", out)
	}
	out = callRemote(context.Background(), client, endpoint, "fixture-bearer", body)
	encoded, _ := json.Marshal(out)
	if out.success != nil || out.Code != "http-failed" || out.Status != 307 || redirectCalls != 0 || strings.Contains(string(encoded), "PRIVATE") || strings.Contains(string(encoded), "fixture-bearer") {
		t.Fatalf("redirect/privacy failed: %+v", out)
	}
	untrusted := newRemoteClient()
	defer untrusted.CloseIdleConnections()
	if out := callRemote(context.Background(), untrusted, endpoint, "fixture-bearer", body); out.success != nil || out.Code != "transport-failed" {
		t.Fatal("untrusted certificate accepted")
	}
}

func TestRemoteOnlyVerifiedBytesAreSent(t *testing.T) {
	t.Setenv("PHOTOSCRAWL_MODEL_API_KEY", "fixture-bearer")
	f := newRemoteFixture(t)
	plan := f.planNow(t)
	client := &http.Client{Transport: syntheticRoundTripper(func(r *http.Request) (*http.Response, error) {
		if err := os.WriteFile(f.image, []byte("replaced after verification"), 0600); err != nil {
			t.Fatal(err)
		}
		var request openAIChatCompletionRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatal(err)
		}
		_, encoded, ok := strings.Cut(request.Messages[0].Content[1].ImageURL.URL, ",")
		if !ok {
			t.Fatal("missing image data")
		}
		data, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			t.Fatal(err)
		}
		if remoteDigest(data) != plan.Entries[0].SHA256 {
			t.Fatal("request reread changed resource")
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(remoteEnvelope(remoteTestPayload, "stop"))), Request: r}, nil
	})}
	if result, err := Classify(context.Background(), f.paths, f.executeOptions(client)); err != nil || result.ContentClassified != 1 {
		t.Fatalf("verified bytes: %v %+v", err, result)
	}
}

func TestRemoteLimitUsesFrozenPrefix(t *testing.T) {
	t.Setenv("PHOTOSCRAWL_MODEL_API_KEY", "fixture-bearer")
	for _, classified := range []bool{false, true} {
		t.Run(fmt.Sprint(classified), func(t *testing.T) {
			f := newRemoteFixture(t)
			var unrelated string
			remoteFixtureSQL(t, f, func(db *sql.DB) {
				if err := db.QueryRow(`select id from asset where local_identifier='synthetic-unrelated'`).Scan(&unrelated); err != nil {
					t.Fatal(err)
				}
			})
			data, _ := json.Marshal([]string{f.ids[1], f.ids[0], unrelated})
			if err := os.WriteFile(f.options.RemoteModel.AssetIDsFile, data, 0600); err != nil {
				t.Fatal(err)
			}
			if result, err := Classify(context.Background(), f.paths, f.options); err != nil || result.Remote.Planned != 2 {
				t.Fatalf("two planned uploads: %v", err)
			}
			if classified {
				remoteFixtureSQL(t, f, func(db *sql.DB) {
					if _, err := db.Exec(`update classification_queue set state='content_classified' where asset_id=?`, f.ids[0]); err != nil {
						t.Fatal(err)
					}
				})
			}
			calls := 0
			opts := f.executeOptions(fixtureRemoteClient(t, remoteEnvelope(remoteTestPayload, "stop"), 200, &calls))
			opts.Limit = 1
			result, err := Classify(context.Background(), f.paths, opts)
			if err != nil || calls > 1 || result.Remote.Planned != 1 || (calls == 0) != classified {
				t.Fatalf("prefix: %v calls=%d %+v", err, calls, result)
			}
			remoteFixtureSQL(t, f, func(db *sql.DB) {
				var state string
				_ = db.QueryRow(`select state from classification_queue where asset_id=?`, unrelated).Scan(&state)
				if state != "pending" {
					t.Fatal("expanded prefix")
				}
			})
		})
	}
}

func TestRemotePlanningNeverReadsKeyAndInvalidKeyPrecedesArchive(t *testing.T) {
	t.Setenv("PHOTOSCRAWL_MODEL_API_KEY", "invalid key with spaces")
	f := newRemoteFixture(t)
	f.planNow(t)
	missing := Paths{Database: filepath.Join(t.TempDir(), "missing.sqlite")}
	if _, err := Classify(context.Background(), missing, f.executeOptions(nil)); err == nil || err.Error() != "invalid-remote-key:PHOTOSCRAWL_MODEL_API_KEY" {
		t.Fatalf("key validation: %v", err)
	}
	if _, err := os.Stat(missing.Database); !os.IsNotExist(err) {
		t.Fatal("invalid key opened archive")
	}
}

func TestRemoteClaimTimeDriftHasZeroUploadEvidence(t *testing.T) {
	t.Setenv("PHOTOSCRAWL_MODEL_API_KEY", "fixture-bearer")
	f := newRemoteFixture(t)
	var unrelated string
	remoteFixtureSQL(t, f, func(db *sql.DB) {
		if err := db.QueryRow(`select id from asset where local_identifier='synthetic-unrelated'`).Scan(&unrelated); err != nil {
			t.Fatal(err)
		}
	})
	data, _ := json.Marshal([]string{f.ids[0], unrelated})
	if err := os.WriteFile(f.options.RemoteModel.AssetIDsFile, data, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Classify(context.Background(), f.paths, f.options); err != nil {
		t.Fatal(err)
	}
	calls := 0
	client := &http.Client{Transport: syntheticRoundTripper(func(r *http.Request) (*http.Response, error) {
		calls++
		if err := os.WriteFile(f.image, []byte("changed while first request runs"), 0600); err != nil {
			t.Fatal(err)
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(remoteEnvelope(remoteTestPayload, "stop"))), Request: r}, nil
	})}
	result, err := Classify(context.Background(), f.paths, f.executeOptions(client))
	if err != nil || calls != 1 || result.Remote.Skipped != 1 || result.Remote.CloudTransmitted != nil || result.Remote.UnknownTransmissionAttempts != 1 || result.Remote.ConfirmedPayloadWrites != 0 {
		t.Fatalf("claim-time drift: %v %+v calls=%d", err, result, calls)
	}
	remoteFixtureSQL(t, f, func(db *sql.DB) {
		var value string
		if err := db.QueryRow(`select value_json from evidence_ref where source=? and asset_id=?`, remoteSource, unrelated).Scan(&value); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(value, `"request_attempts":0`) || !strings.Contains(value, `"cloud_transmitted":false`) || strings.Contains(value, `"raw_response"`) {
			t.Fatal("zero upload provenance")
		}
	})
}

func TestRemoteStorageFailureRollsBackPrivateSuccess(t *testing.T) {
	t.Setenv("PHOTOSCRAWL_MODEL_API_KEY", "fixture-bearer")
	f := newRemoteFixture(t)
	f.planNow(t)
	remoteFixtureSQL(t, f, func(db *sql.DB) {
		if _, err := db.Exec(`create trigger reject_remote before insert on model_observation when new.source='remote_multimodal' begin select raise(abort,'private-database-error'); end`); err != nil {
			t.Fatal(err)
		}
	})
	calls := 0
	body := remoteEnvelope(strings.Replace(remoteTestPayload, "A synthetic scene.", "successful-private-sentinel", 1), "stop")
	_, err := Classify(context.Background(), f.paths, f.executeOptions(fixtureRemoteClient(t, body, 200, &calls)))
	if err == nil || err.Error() != "remote-storage-failed" || calls != 1 {
		t.Fatalf("storage failure: %v", err)
	}
	remoteFixtureSQL(t, f, func(db *sql.DB) {
		var count int
		_ = db.QueryRow(`select count(*) from evidence_ref where source=?`, remoteSource).Scan(&count)
		if count != 0 {
			t.Fatal("success evidence escaped rollback")
		}
		_ = db.QueryRow(`select count(*) from model_observation`).Scan(&count)
		if count != 0 {
			t.Fatal("observations escaped rollback")
		}
		var value string
		if err := db.QueryRow(`select metadata_json from model_run where source=?`, remoteSource).Scan(&value); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(value, "successful-private-sentinel") || strings.Contains(value, "private-database-error") || !strings.Contains(value, "storage-failed") {
			t.Fatal("storage failure metadata leaked")
		}
	})
}

func TestRemoteHTTPFailuresDiscardEveryProviderField(t *testing.T) {
	t.Setenv("PHOTOSCRAWL_MODEL_API_KEY", "fixture-bearer")
	for _, status := range []int{301, 302, 307, 308, 400, 401, 422, 429, 500} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			f := newRemoteFixture(t)
			f.planNow(t)
			calls := 0
			body := `{"error":{"message":"fixture-bearer data:image/jpeg;base64,SENTINELIMAGE OCR-SENTINEL LOCATION-SENTINEL"},"nested":{"secret":"PRIVATE-SENTINEL"}}`
			result, err := Classify(context.Background(), f.paths, f.executeOptions(fixtureRemoteClient(t, body, status, &calls)))
			if err != nil || calls != 1 || result.ContentClassificationFailures != 1 {
				t.Fatalf("HTTP failure: %v %+v", err, result)
			}
			check := func(value string) {
				for _, sentinel := range []string{"fixture-bearer", "SENTINELIMAGE", "OCR-SENTINEL", "LOCATION-SENTINEL", "PRIVATE-SENTINEL"} {
					if strings.Contains(value, sentinel) {
						t.Fatal("remote provider text leaked")
					}
				}
			}
			encoded, _ := json.Marshal(result)
			check(string(encoded))
			remoteFixtureSQL(t, f, func(db *sql.DB) {
				for _, query := range []string{`select reason from classification_queue`, `select value_json from evidence_ref`, `select metadata_json from model_run`} {
					rows, err := db.Query(query)
					if err != nil {
						t.Fatal(err)
					}
					for rows.Next() {
						var value string
						_ = rows.Scan(&value)
						check(value)
					}
					rows.Close()
				}
			})
		})
	}
}
