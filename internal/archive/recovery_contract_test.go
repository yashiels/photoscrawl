package archive

import (
	"context"
	"encoding/json"
	"github.com/openclaw/photoscrawl/internal/photos"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRecoveryContractRejectsUnpairedBeforeMutation(t *testing.T) {
	var opts ClassifyOptions
	if err := json.Unmarshal([]byte(`{"RecoveryReservationOwner":"recovery:synthetic:reservation","Limit":1,"LocalModel":"fixture"}`), &opts); err != nil {
		t.Fatal(err)
	}
	database := filepath.Join(t.TempDir(), "must-not-exist.sqlite")
	if _, err := Classify(context.Background(), Paths{Database: database}, opts); err == nil {
		t.Error("unpaired recovery reservation was accepted")
	}
	if _, err := os.Stat(database); !os.IsNotExist(err) {
		t.Errorf("invalid recovery accessed archive: %v", err)
	}
}

func TestRecoveryContractConsumesOnlyReservedCohort(t *testing.T) {
	paths := testPaths(t)
	library := filepath.Join(t.TempDir(), "Synthetic.photoslibrary")
	if err := mkdirLibrary(library); err != nil {
		t.Fatal(err)
	}
	if _, err := Crawl(context.Background(), paths, CrawlOptions{LibraryPath: library, Provider: fakeProvider{snapshot: photos.LibrarySnapshot{Provider: "fake", AuthorizationStatus: "authorized", Assets: []photos.Asset{{LocalIdentifier: "synthetic-contract", MediaType: "image", Resources: []photos.Resource{{SourceIdentifier: "synthetic-resource", Type: "photo", UTI: "public.jpeg", LocalPath: writeModelTestImage(t), AvailableLocally: true}}}}}}}); err != nil {
		t.Fatal(err)
	}
	store, err := openArchiveStore(context.Background(), paths.Database)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	expiry := time.Now().UTC().Add(10 * time.Minute).Format(time.RFC3339Nano)
	if _, err := store.DB().Exec(`update classification_queue set claim_owner='recovery:synthetic:reservation',claim_expires_at=?`, expiry); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var state string
		if err := store.DB().QueryRow(`select state from classification_queue`).Scan(&state); err != nil || state != "recovery_started" {
			t.Errorf("request without durable marker: %s %v", state, err)
		}
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"{\"scene_summary\":\"Synthetic.\"}"},"finish_reason":"stop"}]}`))
	}))
	defer server.Close()
	data, _ := json.Marshal(map[string]any{"RecoveryReservationOwner": "recovery:synthetic:reservation", "RecoveryClaimOwner": "recovery:synthetic:worker", "Limit": 1, "LocalModel": "fixture", "LocalModelAPI": "openai", "LocalModelURL": server.URL})
	var opts ClassifyOptions
	if err := json.Unmarshal(data, &opts); err != nil {
		t.Fatal(err)
	}
	result, err := Classify(context.Background(), paths, opts)
	if err != nil || result.ContentClassified != 1 {
		t.Fatalf("reserved cohort not classified exactly once: %+v %v", result, err)
	}
	var owner, actualExpiry string
	if err := store.DB().QueryRow(`select claim_owner,claim_expires_at from classification_queue`).Scan(&owner, &actualExpiry); err != nil || owner != "recovery:synthetic:worker" || actualExpiry != expiry {
		t.Fatalf("reservation ownership/expiry not retained: %s %s %v", owner, actualExpiry, err)
	}
}
