package archive

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"testing"

	"github.com/openclaw/photoscrawl/internal/photos"
)

func discoveryCrawl(t *testing.T, paths Paths, library string, sequence int, newOnly bool, provider string, assets ...photos.Asset) (CrawlResult, error) {
	t.Helper()
	return Crawl(context.Background(), paths, CrawlOptions{LibraryPath: library, NewOnly: newOnly,
		Provider: fakeProvider{snapshot: photos.LibrarySnapshot{Provider: provider, Assets: assets}},
		Now:      fixedClock(fmt.Sprintf("2026-10-15T12:00:%02dZ", sequence))})
}

func discoveryState(t *testing.T, db *sql.DB, filter string) string {
	t.Helper()
	var state strings.Builder
	for _, table := range []string{"asset", "asset_resource", "album_membership", "location_observation", "evidence_ref", "text_observation", "classification_queue", "crawl_seen_asset", "asset_fts"} {
		column := "asset_id"
		if table == "asset" || table == "asset_fts" {
			column = "id"
		}
		selection := "*"
		if table == "asset_fts" {
			selection = "rowid, *"
		}
		rows, err := db.Query("select " + selection + " from " + table + " where " + column + " in (select id from asset where " + filter + ") order by 1")
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			values, pointers := make([]any, len(columns)), make([]any, len(columns))
			for i := range values {
				pointers[i] = &values[i]
			}
			if err := rows.Scan(pointers...); err != nil {
				t.Fatal(err)
			}
			fmt.Fprintf(&state, "%s:%#v\n", table, values)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
	}
	return state.String()
}

func TestDiscoveryPreservesExistingAndDefersFullRefresh(t *testing.T) {
	for _, newOnly := range []bool{false, true} {
		t.Run(fmt.Sprintf("new-only=%t", newOnly), func(t *testing.T) {
			paths, library := testPaths(t), t.TempDir()
			initial := fakeSnapshot(false, true).Assets
			deleted := identityFixtureAsset("fixture", "known-deleted")
			deleted.DeletionReason = "synthetic-deletion"
			initial = append(initial, deleted)
			if _, err := discoveryCrawl(t, paths, library, 0, false, "fixture", initial...); err != nil {
				t.Fatal(err)
			}
			db := identityFixtureDB(t, paths)
			execTestSQL(t, db, `update classification_queue set state='done', reason='synthetic-processed';
 update classification_queue set state='pending', claim_owner='synthetic-worker', claim_expires_at='2026-10-16T00:00:00Z' where asset_id=(select id from asset where local_identifier='fixture-asset-1');
 delete from crawl_seen_asset where asset_id=(select id from asset where local_identifier='fixture-asset-2');
 insert into text_observation select 'synthetic-observation', asset_id, 'synthetic-text', 1, '{}', 'en', 'fixture', id from evidence_ref where evidence_kind='asset_metadata' limit 1`)
			before := discoveryState(t, db, "1=1")
			var marker, cursor string
			if err := db.QueryRow(`select snapshot_path from source_library`).Scan(&marker); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow(`select cursor from sync_state`).Scan(&cursor); err != nil {
				t.Fatal(err)
			}
			initial[0].Albums[0].AlbumTitle = "ChangedSyntheticAlbum"
			initial[0].Favorite = true
			initial[1].DeletionReason = "synthetic-deletion"
			initial[2].DeletionReason = ""
			fresh := identityFixtureAsset("fixture", "fresh-asset")
			fresh.Albums = []photos.AlbumMembership{{AlbumID: "fresh-album", AlbumTitle: "FreshSyntheticAlbum"}}
			fresh.Location = &photos.Location{Latitude: 1, Longitude: 2}
			tombstone := identityFixtureAsset("fixture", "fresh-tombstone")
			tombstone.DeletionReason = "synthetic-deletion"
			tombstone.DeletedAt = "2026-10-15T11:00:00Z"
			incoming := append(initial, fresh, fresh, tombstone)
			result, err := discoveryCrawl(t, paths, library, 1, newOnly, "fixture", incoming...)
			if err != nil {
				t.Fatal(err)
			}
			if !newOnly {
				if result.QueuedForClassify != 3 || result.AssetsRestored != 1 || result.AssetsDeleted != 2 {
					t.Fatalf("normal invalidation: %#v", result)
				}
				assertCount(t, db, `select count(*) from classification_queue where state='pending' and claim_owner=''`, 3)
				t.Logf("normal metadata changes requeue processed assets: %#v", result)
				return
			}
			if result.Mode != "new_only" || result.AssetsNew != 2 || result.AssetsSkippedExisting != 4 || result.QueuedForClassify != 1 || result.AssetsChanged != 0 || result.AssetsRestored != 0 || result.AssetsDeleted != 1 || result.PreviouslySeenMissing != 0 {
				t.Fatalf("discovery counts: %#v", result)
			}
			if after := discoveryState(t, db, "local_identifier not in ('fresh-asset','fresh-tombstone')"); after != before {
				t.Fatal("discovery mutated existing rows")
			}
			assertCount(t, db, `select count(*) from asset`, 5)
			assertCount(t, db, `select count(*) from asset_resource where asset_id=(select id from asset where local_identifier='fresh-asset')`, 1)
			assertCount(t, db, `select count(*) from location_observation where asset_id=(select id from asset where local_identifier='fresh-asset')`, 1)
			assertSearchCount(t, context.Background(), paths, "FreshSyntheticAlbum", 1)
			assertSearchCount(t, context.Background(), paths, "ChangedSyntheticAlbum", 0)
			var gotMarker, gotCursor, mode string
			if err := db.QueryRow(`select snapshot_path from source_library`).Scan(&gotMarker); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow(`select cursor from sync_state`).Scan(&gotCursor); err != nil {
				t.Fatal(err)
			}
			if marker != gotMarker || cursor != gotCursor {
				t.Fatal("discovery advanced full refresh markers")
			}
			if err := db.QueryRow(`select json_extract(metadata_json,'$.mode') from crawl_snapshot where id=?`, result.SnapshotID).Scan(&mode); err != nil || mode != "new_only" {
				t.Fatalf("snapshot mode %q: %v", mode, err)
			}
			allBefore := discoveryState(t, db, "1=1")
			result, err = discoveryCrawl(t, paths, library, 2, true, "fixture", incoming...)
			if err != nil {
				t.Fatal(err)
			}
			if result.AssetsNew != 0 || result.QueuedForClassify != 0 || result.AssetsSkippedExisting != 6 || discoveryState(t, db, "1=1") != allBefore {
				t.Fatalf("repeat changed archive: %#v", result)
			}
			result, err = discoveryCrawl(t, paths, library, 3, false, "fixture", incoming...)
			if err != nil {
				t.Fatal(err)
			}
			if result.AssetsChanged != 2 || result.AssetsRestored != 1 || result.AssetsDeleted != 1 || result.QueuedForClassify != 2 {
				t.Fatalf("deferred full refresh: %#v", result)
			}
			assertSearchCount(t, context.Background(), paths, "ChangedSyntheticAlbum", 1)
			t.Logf("discovery preserved old rows; later full crawl applied deferred changes: %#v", result)
		})
	}
}

func TestDiscoveryIdentityAliasesAndRollback(t *testing.T) {
	for _, first := range []string{"photokit", "photos_sqlite_snapshot"} {
		t.Run(first, func(t *testing.T) {
			paths, library := testPaths(t), filepath.Join(t.TempDir(), "Synthetic.photoslibrary")
			if err := mkdirLibrary(library); err != nil {
				t.Fatal(err)
			}
			second, identifier, alternate := "photokit", identityFixtureUUID, identityFixtureUUID+"/L0/001"
			if first == "photokit" {
				second, identifier, alternate = "photos_sqlite_snapshot", alternate, identifier
			}
			asset := identityFixtureAsset(first, identifier)
			if _, err := discoveryCrawl(t, paths, library, 0, false, first, asset); err != nil {
				t.Fatal(err)
			}
			db := identityFixtureDB(t, paths)
			before := discoveryState(t, db, "1=1")
			result, err := discoveryCrawl(t, paths, library, 1, true, second, identityFixtureAsset(second, alternate))
			if err != nil {
				t.Fatal(err)
			}
			if result.AssetsNew != 0 || result.AssetsSkippedExisting != 1 || discoveryState(t, db, "1=1") != before {
				t.Fatalf("alias changed archive: %#v", result)
			}
			execTestSQL(t, db, `insert into asset select 'ambiguous', ?, media_type,media_subtypes,creation_date,modification_date,added_date,timezone_name,width,height,duration_seconds,favorite,hidden,burst_identifier,represents_burst,source_library_id,metadata_json,deleted_at,deletion_source,deletion_reason from asset limit 1`, alternate)
			rollbackBefore := identityFixtureState(t, paths)
			_, err = discoveryCrawl(t, paths, library, 2, true, first, identityFixtureAsset(first, "new-before-ambiguity"), asset)
			if err == nil || !strings.Contains(err.Error(), "ambiguous asset identity") {
				t.Fatalf("ambiguity: %v", err)
			}
			if identityFixtureState(t, paths) != rollbackBefore {
				t.Fatal("ambiguity did not roll back")
			}
		})
	}
}

func TestDiscoveryInvalidNewAssetRollsBack(t *testing.T) {
	for _, invalid := range []string{"metadata", "tombstone", "resource", "identity", "identifier"} {
		t.Run(invalid, func(t *testing.T) {
			paths, library := testPaths(t), t.TempDir()
			good := identityFixtureAsset("photos_sqlite_snapshot", "synthetic-good")
			if _, err := discoveryCrawl(t, paths, library, 0, true, "photos_sqlite_snapshot", good); err != nil {
				t.Fatal(err)
			}
			before := identityFixtureState(t, paths)
			bad := identityFixtureAsset("photos_sqlite_snapshot", "synthetic-bad")
			switch invalid {
			case "metadata":
				bad.Metadata["invalid"] = math.NaN()
			case "tombstone":
				bad.DeletedAt = "invalid"
			case "resource":
				bad.Resources[0].SourceIdentifier = ""
			case "identifier":
				bad.LocalIdentifier = " "
			case "identity":
				bad.LocalIdentifier = identityFixtureUUID
				bad.Metadata = nil
			}
			_, err := discoveryCrawl(t, paths, library, 1, true, "photos_sqlite_snapshot", identityFixtureAsset("photos_sqlite_snapshot", "synthetic-before-invalid"), bad)
			if err == nil {
				t.Fatal("invalid new asset accepted")
			}
			if identityFixtureState(t, paths) != before {
				t.Fatal("invalid input did not roll back")
			}
			db := identityFixtureDB(t, paths)
			assertCount(t, db, `select count(*) from sync_state`, 0)
		})
	}
}

func TestDiscoveryPreservesMissingAndUnseenKnownAssets(t *testing.T) {
	paths, library := testPaths(t), t.TempDir()
	old := identityFixtureAsset("photos_sqlite_snapshot", identityFixtureUUID)
	if _, err := discoveryCrawl(t, paths, library, 0, false, "photos_sqlite_snapshot", old); err != nil {
		t.Fatal(err)
	}
	db := identityFixtureDB(t, paths)
	execTestSQL(t, db, `delete from crawl_seen_asset`)
	before := discoveryState(t, db, "1=1")
	result, err := discoveryCrawl(t, paths, library, 1, true, "photos_sqlite_snapshot")
	if err != nil || result.PreviouslySeenMissing != 0 || discoveryState(t, db, "1=1") != before {
		t.Fatalf("missing known asset changed: %#v %v", result, err)
	}
	old.Resources[0].SourceIdentifier = ""
	old.DeletedAt = "invalid-deferred-tombstone"
	result, err = discoveryCrawl(t, paths, library, 2, true, "photos_sqlite_snapshot", old)
	if err != nil || result.AssetsSkippedExisting != 1 || discoveryState(t, db, "1=1") != before {
		t.Fatalf("unseen exact existing asset changed: %#v %v", result, err)
	}
	beforeRollback := identityFixtureState(t, paths)
	_, err = discoveryCrawl(t, paths, library, 3, true, "photokit", identityFixtureAsset("photokit", identityFixtureUUID+"/L0/001"))
	if err == nil || !strings.Contains(err.Error(), "unambiguous cross-provider evidence") || identityFixtureState(t, paths) != beforeRollback {
		t.Fatalf("unproved alias: %v", err)
	}
}
