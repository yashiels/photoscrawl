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
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openclaw/photoscrawl/internal/photos"
)

const fixtureReservation = "recovery:synthetic-run:reservation"

func recoveryFixture(t *testing.T, count int) (Paths, *sql.DB, string) {
	t.Helper()
	paths := testPaths(t)
	library := filepath.Join(t.TempDir(), "Synthetic.photoslibrary")
	if err := mkdirLibrary(library); err != nil {
		t.Fatal(err)
	}
	assets := make([]photos.Asset, count)
	for i := range assets {
		image := writeModelTestImage(t)
		if err := os.WriteFile(image, []byte(fmt.Sprintf("synthetic-%d", i)), 0o600); err != nil {
			t.Fatal(err)
		}
		assets[i] = photos.Asset{LocalIdentifier: fmt.Sprintf("synthetic-%d", i), MediaType: "image", Resources: []photos.Resource{{SourceIdentifier: fmt.Sprintf("resource-%d", i), Type: "photo", UTI: "public.jpeg", LocalPath: image, AvailableLocally: true}}}
	}
	if _, err := Crawl(context.Background(), paths, CrawlOptions{LibraryPath: library, Provider: fakeProvider{snapshot: photos.LibrarySnapshot{Provider: "fake", AuthorizationStatus: "authorized", Assets: assets}}}); err != nil {
		t.Fatal(err)
	}
	store, err := openArchiveStore(context.Background(), paths.Database)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	expiry := time.Now().UTC().Add(10 * time.Minute).Format(time.RFC3339Nano)
	recoveryExec(t, store.DB(), `update classification_queue set claim_owner=?, claim_expires_at=?`, fixtureReservation, expiry)
	return paths, store.DB(), expiry
}

func recoveryExec(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()
	if _, err := db.Exec(query, args...); err != nil {
		t.Fatal(err)
	}
}

func recoveryOpts(url, participant string) ClassifyOptions {
	return ClassifyOptions{Limit: 1000, LocalModel: "fixture", LocalModelAPI: "openai", LocalModelURL: url, RecoveryReservationOwner: fixtureReservation, RecoveryClaimOwner: "recovery:synthetic-run:" + participant}
}

func recoveryReply(w http.ResponseWriter) {
	_ = json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": map[string]any{"content": `{"scene_summary":"Synthetic scene.","objects":["synthetic object"]}`}, "finish_reason": "stop"}}})
}

func receiveRecovery[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(15 * time.Second):
		t.Fatal("recovery barrier timed out")
		var zero T
		return zero
	}
}

func recoverySnapshot(t *testing.T, db *sql.DB, tables ...string) string {
	t.Helper()
	var snapshot []any
	for _, table := range tables {
		rows, err := db.Query(`select * from ` + table + ` order by 1`)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		var records [][]any
		for rows.Next() {
			values := make([]any, len(columns))
			refs := make([]any, len(columns))
			for i := range values {
				refs[i] = &values[i]
			}
			if err := rows.Scan(refs...); err != nil {
				t.Fatal(err)
			}
			records = append(records, values)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		_ = rows.Close()
		snapshot = append(snapshot, records)
	}
	data, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

var recoveryObservationTables = []string{"visual_observation", "model_observation", "observation_fts", "observation_fts_rowid", "observation_term", "evidence_ref", "model_run"}

func TestRecoveryConcurrentReservationExclusion(t *testing.T) {
	paths, db, expiry := recoveryFixture(t, 5)
	recoveryExec(t, db, `update classification_queue set claim_owner='',claim_expires_at=null where asset_id=(select id from asset where local_identifier='synthetic-4')`)
	entered := make(chan string, 8)
	requested := make(chan string, 8)
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	var recoveryCalls, foreignCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Model string `json:"model"`
		}
		data, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(data, &body)
		for i := 0; i < 5; i++ {
			id := fmt.Sprintf("synthetic-%d", i)
			if strings.Contains(string(data), base64.StdEncoding.EncodeToString([]byte(id))) {
				requested <- body.Model + ":" + id
			}
		}
		if body.Model == "foreign" {
			foreignCalls.Add(1)
		} else {
			recoveryCalls.Add(1)
		}
		entered <- body.Model
		select {
		case <-release:
			recoveryReply(w)
		case <-r.Context().Done():
		}
	}))
	defer server.Close()
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	errors := make(chan error, 4)
	for _, owner := range []string{"worker-a", "worker-b"} {
		go func() {
			_, err := Classify(context.Background(), paths, recoveryOpts(server.URL, owner))
			errors <- err
		}()
	}
	for i := 0; i < 2; i++ {
		if receiveRecovery(t, entered) != "fixture" {
			t.Fatal("unexpected foreign request")
		}
	}
	var markers int
	if err := db.QueryRow(`select count(*) from classification_queue where state='recovery_started' and reason='recovery_attempt_started' and claim_expires_at=?`, expiry).Scan(&markers); err != nil || markers != 2 {
		t.Fatalf("durable markers=%d err=%v", markers, err)
	}
	for i := 0; i < 2; i++ {
		go func() {
			_, err := Classify(context.Background(), paths, ClassifyOptions{Limit: 1000, LocalModel: "foreign", LocalModelAPI: "openai", LocalModelURL: server.URL})
			errors <- err
		}()
	}
	if receiveRecovery(t, entered) != "foreign" {
		t.Fatal("ordinary classifier did not get unrelated row")
	}
	close(release)
	for i := 0; i < 4; i++ {
		if err := receiveRecovery(t, errors); err != nil {
			t.Fatal(err)
		}
	}
	if recoveryCalls.Load() != 4 || foreignCalls.Load() != 1 {
		t.Fatalf("recovery=%d foreign=%d", recoveryCalls.Load(), foreignCalls.Load())
	}
	seen := map[string]bool{}
	for i := 0; i < 5; i++ {
		id := receiveRecovery(t, requested)
		if seen[id] {
			t.Fatalf("duplicate asset request: %s", id)
		}
		seen[id] = true
	}
	for i := 0; i < 4; i++ {
		if !seen[fmt.Sprintf("fixture:synthetic-%d", i)] {
			t.Fatal("recovery missed cohort or foreign claimed cohort", seen)
		}
	}
	if !seen["foreign:synthetic-4"] {
		t.Fatal("recovery widened cohort", seen)
	}
	var owned int
	if err := db.QueryRow(`select count(*) from classification_queue where state='content_classified' and claim_owner in ('recovery:synthetic-run:worker-a','recovery:synthetic-run:worker-b') and claim_expires_at=?`, expiry).Scan(&owned); err != nil || owned != 4 {
		t.Fatalf("owned=%d err=%v", owned, err)
	}
	result, err := Classify(context.Background(), paths, recoveryOpts(server.URL, "worker-c"))
	if err != nil || result.Processed != 0 {
		t.Fatalf("empty cohort: %+v %v", result, err)
	}
	if recoveryCalls.Load() != 4 {
		t.Fatal("empty recovery sent HTTP")
	}
}

func TestRecoveryOutcomeDriftAndRollback(t *testing.T) {
	for _, drift := range []string{"owner", "fingerprint", "expiry", "state", "id", "storage", "cancel"} {
		t.Run(drift, func(t *testing.T) {
			paths, db, _ := recoveryFixture(t, 1)
			recoveryExec(t, db, `update classification_queue set claim_owner='',claim_expires_at=null`)
			seed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { recoveryReply(w) }))
			if _, err := Classify(context.Background(), paths, ClassifyOptions{Limit: 1, LocalModel: "fixture", LocalModelAPI: "openai", LocalModelURL: seed.URL}); err != nil {
				t.Fatal(err)
			}
			seed.Close()
			before := recoverySnapshot(t, db, recoveryObservationTables...)
			recoveryExec(t, db, `update classification_queue set state='pending',claim_owner=?,claim_expires_at=?`, fixtureReservation, time.Now().UTC().Add(10*time.Minute).Format(time.RFC3339Nano))
			entered := make(chan struct{}, 1)
			release := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				entered <- struct{}{}
				select {
				case <-release:
					recoveryReply(w)
				case <-r.Context().Done():
				}
			}))
			defer server.Close()
			defer func() {
				select {
				case <-release:
				default:
					close(release)
				}
			}()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { _, err := Classify(ctx, paths, recoveryOpts(server.URL, "worker-a")); done <- err }()
			receiveRecovery(t, entered)
			var state, reason string
			if err := db.QueryRow(`select state,reason from classification_queue`).Scan(&state, &reason); err != nil || state != "recovery_started" || reason != "recovery_attempt_started" {
				t.Fatalf("marker %s %s %v", state, reason, err)
			}
			if before != recoverySnapshot(t, db, recoveryObservationTables...) {
				t.Fatal("marker changed observations before inference completed")
			}
			switch drift {
			case "owner":
				recoveryExec(t, db, `update classification_queue set claim_owner='competing-owner'`)
			case "fingerprint":
				recoveryExec(t, db, `update classification_queue set input_fingerprint='new-input'`)
			case "expiry":
				recoveryExec(t, db, `update classification_queue set claim_expires_at=?`, time.Now().UTC().Add(11*time.Minute).Format(time.RFC3339Nano))
			case "state":
				recoveryExec(t, db, `update classification_queue set state='content_failed',reason='competing-state'`)
			case "id":
				recoveryExec(t, db, `update classification_queue set id=id||'-changed'`)
			case "storage":
				recoveryExec(t, db, `create trigger synthetic_storage_failure before insert on model_run begin select raise(abort,'synthetic later storage failure'); end`)
			case "cancel":
				cancel()
			}
			queueBefore := recoverySnapshot(t, db, "classification_queue")
			close(release)
			err := receiveRecovery(t, done)
			if err == nil || !strings.Contains(err.Error(), "recovery incomplete") {
				t.Fatalf("expected explicit incomplete, got %v", err)
			}
			if before != recoverySnapshot(t, db, recoveryObservationTables...) {
				t.Fatal("stale/failed outcome changed observations, FTS, terms, evidence or model run")
			}
			if queueBefore != recoverySnapshot(t, db, "classification_queue") {
				t.Fatal("stale/failed outcome changed competing queue")
			}
		})
	}
}

func TestRecoveryBoundsAndLostBeforeMarker(t *testing.T) {
	for _, duration := range []time.Duration{-time.Minute, 20 * time.Second, 3 * time.Hour} {
		t.Run(duration.String(), func(t *testing.T) {
			paths, db, _ := recoveryFixture(t, 1)
			recoveryExec(t, db, `update classification_queue set claim_expires_at=?`, time.Now().UTC().Add(duration).Format(time.RFC3339Nano))
			before := recoverySnapshot(t, db, "classification_queue")
			calls := atomic.Int32{}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); recoveryReply(w) }))
			defer server.Close()
			_, err := Classify(context.Background(), paths, recoveryOpts(server.URL, "worker-a"))
			if duration > 0 && err == nil {
				t.Fatal("invalid live lease accepted")
			}
			if calls.Load() != 0 || before != recoverySnapshot(t, db, "classification_queue") {
				t.Fatal("invalid lease caused inference or mutation")
			}
		})
	}
	paths, db, _ := recoveryFixture(t, 1)
	opts := recoveryOpts("http://127.0.0.1:1", "worker-a")
	store, err := openArchiveStore(context.Background(), paths.Database)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var input *classifyInput
	if err := store.WithTx(context.Background(), func(tx *sql.Tx) error {
		var err error
		input, err = claimRecoveryInput(context.Background(), tx, opts, time.Now().UTC(), "")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	recoveryExec(t, db, `update classification_queue set claim_owner='competing-owner'`)
	before := recoverySnapshot(t, db, "classification_queue")
	expiry, _ := time.Parse(time.RFC3339Nano, input.ClaimExpiresAt)
	ctx, cancel := context.WithDeadline(context.Background(), expiry.Add(-recoveryDeadlineMargin))
	defer cancel()
	if err := store.WithTx(ctx, func(tx *sql.Tx) error { return markRecoveryStarted(ctx, tx, *input, time.Now().UTC()) }); err == nil {
		t.Fatal("lost owner marked started")
	}
	if before != recoverySnapshot(t, db, "classification_queue") {
		t.Fatal("lost marker mutated queue")
	}
}

func TestRecoveryOptionValidationBeforeArchive(t *testing.T) {
	valid := recoveryOpts("http://127.0.0.1:1", "worker-a")
	cases := []ClassifyOptions{}
	for _, change := range []func(*ClassifyOptions){
		func(o *ClassifyOptions) { o.RecoveryClaimOwner = "" }, func(o *ClassifyOptions) { o.RecoveryReservationOwner = "" },
		func(o *ClassifyOptions) { o.RecoveryClaimOwner = "recovery:other:worker" }, func(o *ClassifyOptions) { o.RecoveryClaimOwner = o.RecoveryReservationOwner },
		func(o *ClassifyOptions) { o.RecoveryClaimOwner = "unknown" }, func(o *ClassifyOptions) { o.RecoveryClaimOwner = "recovery:synthetic-run:" + strings.Repeat("a", 33) },
		func(o *ClassifyOptions) { o.Limit = 0 }, func(o *ClassifyOptions) { o.Limit = 1001 }, func(o *ClassifyOptions) { o.All = true },
		func(o *ClassifyOptions) { o.AllowICloudDownloads = true }, func(o *ClassifyOptions) { o.LocalModel = " " },
	} {
		o := valid
		change(&o)
		cases = append(cases, o)
	}
	for i, opts := range cases {
		path := filepath.Join(t.TempDir(), "must-not-exist.sqlite")
		if _, err := Classify(context.Background(), Paths{Database: path}, opts); err == nil {
			t.Fatalf("case %d accepted", i)
		}
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("case %d archive accessed: %v", i, err)
		}
	}
}

func TestRecoveryCrashAndLegacyExclusion(t *testing.T) {
	if os.Getenv("PHOTOSCRAWL_TEST_RECOVERY_CHILD") == "1" {
		_, _ = Classify(context.Background(), Paths{Database: os.Getenv("PHOTOSCRAWL_TEST_RECOVERY_DB")}, recoveryOpts(os.Getenv("PHOTOSCRAWL_TEST_RECOVERY_URL"), "crashed-worker"))
		os.Exit(2)
	}
	paths, db, _ := recoveryFixture(t, 2)
	entered := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		entered <- struct{}{}
		<-r.Context().Done()
	}))
	defer server.Close()
	child := exec.Command(os.Args[0], "-test.run=^TestRecoveryCrashAndLegacyExclusion$")
	child.Env = append(os.Environ(), "PHOTOSCRAWL_TEST_RECOVERY_CHILD=1", "PHOTOSCRAWL_TEST_RECOVERY_DB="+paths.Database, "PHOTOSCRAWL_TEST_RECOVERY_URL="+server.URL)
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = child.Process.Kill() })
	receiveRecovery(t, entered)
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := child.Wait(); err == nil {
		t.Fatal("crash child succeeded")
	}
	recoveryExec(t, db, `update classification_queue set claim_expires_at='2000-01-01T00:00:00Z' where state='recovery_started'`)
	var states, owners string
	if err := db.QueryRow(`select group_concat(state),group_concat(claim_owner) from classification_queue`).Scan(&states, &owners); err != nil || !strings.Contains(states, "recovery_started") || !strings.Contains(owners, "crashed-worker") {
		t.Fatalf("crash marker missing: %s %s %v", states, owners, err)
	}
	before := recoverySnapshot(t, db, "classification_queue")
	var calls atomic.Int32
	probe := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); recoveryReply(w) }))
	defer probe.Close()
	for _, icloud := range []bool{false, true} {
		result, err := Classify(context.Background(), paths, ClassifyOptions{All: true, LocalModel: "fixture", LocalModelAPI: "openai", LocalModelURL: probe.URL, AllowICloudDownloads: icloud})
		if err != nil || result.Processed != 0 {
			t.Fatalf("new retry consumed protected rows: %+v %v", result, err)
		}
	}
	if before != recoverySnapshot(t, db, "classification_queue") || calls.Load() != 0 {
		t.Fatal("new retry mutated protected rows")
	}
	legacy := os.Getenv("PHOTOSCRAWL_TEST_LEGACY_BINARY")
	if legacy == "" {
		t.Log("legacy execution requires explicit PHOTOSCRAWL_TEST_LEGACY_BINARY")
		return
	}
	version, err := exec.Command(legacy, "--version").CombinedOutput()
	if err != nil || strings.TrimSpace(string(version)) != "0.5.1" {
		t.Fatalf("legacy version %q %v", version, err)
	}
	for _, flags := range [][]string{{"--limit", "100"}, {"--all"}, {"--all", "--allow-icloud-downloads"}} {
		args := append([]string{"classify", "--db", paths.Database, "--local-model", "fixture", "--local-model-api", "openai", "--local-model-url", probe.URL, "--json"}, flags...)
		cmd := exec.Command(legacy, args...)
		cmd.Env = append(os.Environ(), "HOME="+t.TempDir())
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("legacy invocation %v: %v %s", args, err, output)
		}
		var result struct {
			Data      ClassifyResult `json:"data"`
			Result    ClassifyResult `json:"result"`
			Processed int            `json:"processed"`
		}
		if err := json.Unmarshal(output, &result); err != nil {
			t.Fatal(err)
		}
		if result.Processed != 0 || result.Data.Processed != 0 || result.Result.Processed != 0 || calls.Load() != 0 {
			t.Fatalf("legacy consumed protected rows: %s", output)
		}
		if !reflect.DeepEqual(before, recoverySnapshot(t, db, "classification_queue")) {
			t.Fatal("legacy mutated protected rows")
		}
		t.Logf("brew 0.5.1 invocation: %s; zero model calls; queue unchanged; output=%s", strings.Join(args, " "), output)
	}
}

func TestRecoveryTerminalFailuresRetainOwnership(t *testing.T) {
	for _, mode := range []string{"read", "http", "unavailable", "waiting"} {
		t.Run(mode, func(t *testing.T) {
			paths, db, expiry := recoveryFixture(t, 1)
			switch mode {
			case "read":
				recoveryExec(t, db, `update asset_resource set local_path=local_path||'-missing.jpeg'`)
			case "unavailable":
				recoveryExec(t, db, `delete from asset_resource`)
			case "waiting":
				recoveryExec(t, db, `update asset_resource set local_path='',available_locally=0,needs_download=1`)
				recoveryExec(t, db, `update classification_queue set needs_download=1`)
			}
			calls := atomic.Int32{}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				http.Error(w, "synthetic HTTP failure", http.StatusBadRequest)
			}))
			defer server.Close()
			result, err := Classify(context.Background(), paths, recoveryOpts(server.URL, "worker-a"))
			if err != nil {
				t.Fatal(err)
			}
			expected := "content_failed"
			if mode == "unavailable" || mode == "waiting" {
				expected = "content_unavailable"
			}
			var state, owner, actualExpiry string
			if err := db.QueryRow(`select state,claim_owner,claim_expires_at from classification_queue`).Scan(&state, &owner, &actualExpiry); err != nil || state != expected || owner != "recovery:synthetic-run:worker-a" || actualExpiry != expiry {
				t.Fatalf("terminal state=%s owner=%s expiry=%s err=%v", state, owner, actualExpiry, err)
			}
			if result.ContentClassified != 0 || result.Processed != 1 {
				t.Fatalf("fake success: %+v", result)
			}
			if mode != "http" && calls.Load() != 0 {
				t.Fatal("non-readable content sent HTTP")
			}
		})
	}
}

func TestRecoveryDeadlinePendingRetentionAndStatus(t *testing.T) {
	paths, db, expiryText := recoveryFixture(t, 1)
	expiry, _ := time.Parse(time.RFC3339Nano, expiryText)
	store, err := openArchiveStore(context.Background(), paths.Database)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	opts := recoveryOpts("http://127.0.0.1:1", "worker-a")
	var input *classifyInput
	if err := store.WithTx(context.Background(), func(tx *sql.Tx) error {
		var err error
		input, err = claimRecoveryInput(context.Background(), tx, opts, time.Now().UTC(), "")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	before := recoverySnapshot(t, db, "classification_queue")
	ctx, cancel := context.WithDeadline(context.Background(), expiry.Add(-recoveryDeadlineMargin))
	defer cancel()
	if err := store.WithTx(ctx, func(tx *sql.Tx) error { return markRecoveryStarted(ctx, tx, *input, expiry.Add(-50*time.Second)) }); err == nil {
		t.Fatal("insufficient marker margin accepted")
	}
	if before != recoverySnapshot(t, db, "classification_queue") {
		t.Fatal("pending ownership lost")
	}
	if err := store.WithTx(ctx, func(tx *sql.Tx) error { return markRecoveryStarted(ctx, tx, *input, time.Now().UTC()) }); err != nil {
		t.Fatal(err)
	}
	before = recoverySnapshot(t, db, "classification_queue")
	if err := store.WithTx(ctx, func(tx *sql.Tx) error { return guardRecoveryOutcome(ctx, tx, *input, expiry.Add(-20*time.Second)) }); err == nil {
		t.Fatal("expired outcome accepted")
	}
	if before != recoverySnapshot(t, db, "classification_queue") {
		t.Fatal("expired guard changed marker")
	}
	status, err := Status(context.Background(), paths)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(status)
	if !strings.Contains(string(data), "recovery_started") || !strings.Contains(string(data), "explicit operator disposition") {
		t.Fatalf("dishonest status: %s", data)
	}
}

func TestRecoveryFixedExpiryAndCallerDeadline(t *testing.T) {
	paths, db, expiry := recoveryFixture(t, 1)
	store, err := openArchiveStore(context.Background(), paths.Database)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	opts := recoveryOpts("http://127.0.0.1:1", "worker-a")
	before := recoverySnapshot(t, db, "classification_queue")
	otherExpiry := time.Now().UTC().Add(11 * time.Minute).Format(time.RFC3339Nano)
	if err := store.WithTx(context.Background(), func(tx *sql.Tx) error {
		_, err := claimRecoveryInput(context.Background(), tx, opts, time.Now().UTC(), otherExpiry)
		return err
	}); err == nil {
		t.Fatal("mixed fixed expiries accepted")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := Classify(ctx, paths, opts); err == nil {
		t.Fatal("insufficient caller deadline accepted")
	}
	if before != recoverySnapshot(t, db, "classification_queue") {
		t.Fatal("failed admission changed queue")
	}
	var input *classifyInput
	if err := store.WithTx(context.Background(), func(tx *sql.Tx) error {
		var err error
		input, err = claimRecoveryInput(context.Background(), tx, opts, time.Now().UTC(), expiry)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.WithTx(context.Background(), func(tx *sql.Tx) error { return markRecoveryStarted(context.Background(), tx, *input, time.Now().UTC()) }); err == nil {
		t.Fatal("unbounded marker context accepted")
	}
}

func TestRecoveryBeforeMarkerLossOrCancelMakesZeroRequests(t *testing.T) {
	for _, mode := range []string{"lost-owner", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			paths, db, expiry := recoveryFixture(t, 1)
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); recoveryReply(w) }))
			defer server.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			beforeMarker := make(chan struct{}, 1)
			resume := make(chan struct{})
			var clockCalls atomic.Int32
			opts := recoveryOpts(server.URL, "worker-a")
			opts.Now = func() time.Time {
				if clockCalls.Add(1) == 3 {
					beforeMarker <- struct{}{}
					<-resume
				}
				return time.Now().UTC()
			}
			done := make(chan error, 1)
			go func() { _, err := Classify(ctx, paths, opts); done <- err }()
			receiveRecovery(t, beforeMarker)
			if mode == "lost-owner" {
				recoveryExec(t, db, `update classification_queue set claim_owner='competing-owner'`)
			} else {
				cancel()
			}
			before := recoverySnapshot(t, db, "classification_queue")
			close(resume)
			if err := receiveRecovery(t, done); err == nil || !strings.Contains(err.Error(), "recovery incomplete") {
				t.Fatalf("expected incomplete: %v", err)
			}
			if calls.Load() != 0 || before != recoverySnapshot(t, db, "classification_queue") {
				t.Fatal("pre-marker failure made request or changed queue")
			}
			var state, owner, actualExpiry string
			if err := db.QueryRow(`select state,claim_owner,claim_expires_at from classification_queue`).Scan(&state, &owner, &actualExpiry); err != nil || state != "pending" || actualExpiry != expiry {
				t.Fatalf("pending claim changed: %s %s %s %v", state, owner, actualExpiry, err)
			}
			if mode == "cancel" && owner != "recovery:synthetic-run:worker-a" {
				t.Fatal("cancel released pending participant claim")
			}
		})
	}
}
