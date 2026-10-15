package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/openclaw/photoscrawl/internal/archive"
)

func TestDiscoveryNativeCLI(t *testing.T) {
	root := t.TempDir()
	library := filepath.Join(root, "Synthetic.photoslibrary")
	source := filepath.Join(library, "database", "Photos.sqlite")
	if err := os.MkdirAll(filepath.Dir(source), 0o700); err != nil {
		t.Fatal(err)
	}
	schema, err := os.ReadFile(filepath.Join("testdata", "curation_photos.sql"))
	if err != nil {
		t.Fatal(err)
	}
	provider, err := sql.Open("sqlite", source)
	if err != nil {
		t.Fatal(err)
	}
	defer provider.Close()
	if _, err := provider.Exec(string(schema)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	binary := os.Getenv("PHOTOSCRAWL_TEST_BINARY")
	if binary == "" {
		binary = filepath.Join(root, "photoscrawl")
		cmd := exec.CommandContext(ctx, "go", "build", "-o", binary, ".")
		cmd.Env = append(os.Environ(), "GOWORK=off")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("build: %v\n%s", err, out)
		}
	}
	database := filepath.Join(root, "archive.sqlite")
	invoke := func(newOnly bool) archive.CrawlResult {
		t.Helper()
		args := []string{"crawl", "--provider", "sqlite", "--library", library, "--db", database, "--json"}
		if newOnly {
			args = append(args, "--new-only")
		}
		out, err := exec.CommandContext(ctx, binary, args...).CombinedOutput()
		if err != nil {
			t.Fatalf("native CLI: %v\n%s", err, out)
		}
		var result archive.CrawlResult
		if err := json.Unmarshal(out, &result); err != nil {
			t.Fatal(err)
		}
		t.Logf("native CLI new_only=%t: %s", newOnly, out)
		return result
	}
	initial := invoke(false)
	if initial.AssetsNew != 3 {
		t.Fatalf("initial: %#v", initial)
	}
	db, err := sql.Open("sqlite", database)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`update classification_queue set state='done',reason='synthetic-processed'`); err != nil {
		t.Fatal(err)
	}
	var marker, cursor, retained string
	if err := db.QueryRow(`select snapshot_path from source_library`).Scan(&marker); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`select cursor from sync_state`).Scan(&cursor); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`select json_group_array(json_array(asset_id,state,reason,input_fingerprint,claim_owner,claim_expires_at)) from (select * from classification_queue order by asset_id)`).Scan(&retained); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Exec(`update ZASSET set ZFAVORITE=1; insert into ZASSET(Z_PK,ZUUID) values(4,'synthetic-new')`); err != nil {
		t.Fatal(err)
	}
	sourceBefore, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	result := invoke(true)
	if result.Mode != "new_only" || result.AssetsNew != 1 || result.AssetsSkippedExisting != 3 || result.QueuedForClassify != 1 || result.PreviouslySeenMissing != 0 {
		t.Fatalf("discovery: %#v", result)
	}
	var markerAfter, cursorAfter, retainedAfter string
	if err := db.QueryRow(`select snapshot_path from source_library`).Scan(&markerAfter); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`select cursor from sync_state`).Scan(&cursorAfter); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`select json_group_array(json_array(asset_id,state,reason,input_fingerprint,claim_owner,claim_expires_at)) from (select * from classification_queue where asset_id in (select id from asset where local_identifier!='synthetic-new') order by asset_id)`).Scan(&retainedAfter); err != nil {
		t.Fatal(err)
	}
	if markerAfter != marker || cursorAfter != cursor || retainedAfter != retained {
		t.Fatalf("discovery changed full markers or existing queue: marker=%t cursor=%t queue=%t\nbefore=%s\nafter=%s", markerAfter == marker, cursorAfter == cursor, retainedAfter == retained, retained, retainedAfter)
	}
	var favorites int
	if err := db.QueryRow(`select sum(favorite) from asset`).Scan(&favorites); err != nil || favorites != 0 {
		t.Fatalf("existing metadata changed: %d %v", favorites, err)
	}
	result = invoke(true)
	if result.AssetsNew != 0 || result.AssetsSkippedExisting != 4 || result.QueuedForClassify != 0 {
		t.Fatalf("repeat: %#v", result)
	}
	result = invoke(false)
	if result.AssetsChanged != 3 || result.QueuedForClassify != 3 {
		t.Fatalf("deferred: %#v", result)
	}
	sourceAfter, err := os.ReadFile(source)
	if err != nil || string(sourceAfter) != string(sourceBefore) {
		t.Fatalf("CLI mutated source fixture: %v", err)
	}
	t.Log("SQLite invariants: retained queue and full-refresh markers unchanged; repeat adds zero; full refresh requeues three deferred changes; source fixture unchanged")
}
