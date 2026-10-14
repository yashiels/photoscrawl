//go:build darwin || linux

package archive

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestRemotePrivateFiles(t *testing.T) {
	root := t.TempDir()
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "plan.json")
	if err := writePrivateRemoteFile(path, []byte(`{"synthetic":true}`)); err != nil {
		t.Fatal(err)
	}
	info, _ := os.Stat(path)
	if info.Mode().Perm() != 0600 {
		t.Fatal("plan permissions")
	}
	if err := writePrivateRemoteFile(path, []byte("overwrite")); err == nil {
		t.Fatal("overwrote plan")
	}
	data, err := readPrivateRemoteFile(path)
	if err != nil || string(data) != `{"synthetic":true}` {
		t.Fatal("plan changed")
	}
	link := filepath.Join(root, "symlink")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if _, err = readPrivateRemoteFile(link); err == nil {
		t.Fatal("symlink accepted")
	}
	if err = writePrivateRemoteFile(link, []byte("overwrite")); err == nil {
		t.Fatal("symlink overwritten")
	}
	parentLink := filepath.Join(root, "parent-link")
	if err = os.Symlink(root, parentLink); err != nil {
		t.Fatal(err)
	}
	if err = writePrivateRemoteFile(filepath.Join(parentLink, "escaped.json"), nil); err == nil {
		t.Fatal("symlink parent accepted")
	}
	if err = os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err = readPrivateRemoteFile(path); err == nil {
		t.Fatal("public manifest accepted")
	}
	if err = os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, []byte(strings.Repeat("x", remotePlanLimit)), 0600); err != nil {
		t.Fatal(err)
	}
	if data, err := readPrivateRemoteFile(path); err != nil || len(data) != remotePlanLimit {
		t.Fatal("exact private input bound rejected")
	}
	if err = os.WriteFile(path, []byte(strings.Repeat("x", remotePlanLimit+1)), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = readPrivateRemoteFile(path); err == nil {
		t.Fatal("oversized private input accepted")
	}
	if err = os.Chmod(root, 0755); err != nil {
		t.Fatal(err)
	}
	if _, err = readPrivateRemoteFile(path); err == nil {
		t.Fatal("public directory accepted")
	}
	if err = writePrivateRemoteFile(filepath.Join(root, "other.json"), nil); err == nil {
		t.Fatal("public output directory accepted")
	}
}

func TestRemoteNonOwnerACLRejected(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("native macOS ACL test")
	}
	root := t.TempDir()
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "plan.json")
	if err := writePrivateRemoteFile(path, []byte("synthetic")); err != nil {
		t.Fatal(err)
	}
	for _, target := range []string{path, root} {
		if out, err := exec.Command("/bin/chmod", "+a", "everyone allow read", target).CombinedOutput(); err != nil {
			t.Fatalf("synthetic ACL: %v %s", err, out)
		}
		if _, err := readPrivateRemoteFile(path); err == nil {
			t.Fatal("non-owner ACL accepted")
		}
		if out, err := exec.Command("/bin/chmod", "-N", target).CombinedOutput(); err != nil {
			t.Fatalf("remove synthetic ACL: %v %s", err, out)
		}
	}
}

func TestRemoteResourceAndRequestBounds(t *testing.T) {
	path := filepath.Join(t.TempDir(), "synthetic.jpg")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.Truncate(remoteResourceLimit); err != nil {
		t.Fatal(err)
	}
	f.Close()
	data, err := readRemoteResource(path)
	if err != nil || len(data) != remoteResourceLimit {
		t.Fatal("exact resource bound rejected")
	}
	config := CompletionConfig{MaxTokens: 2048}
	request, err := remoteRequest("synthetic", data, config)
	if err != nil || len(request) > remoteRequestLimit {
		t.Fatal("encoded expansion rejected")
	}
	f, err = os.OpenFile(path, os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_ = f.Truncate(remoteResourceLimit + 1)
	f.Close()
	if _, err = readRemoteResource(path); err == nil {
		t.Fatal("oversized resource accepted")
	}
	if _, err = remoteRequest("synthetic", append(data, 0), config); err == nil {
		t.Fatal("oversized raw bytes accepted")
	}
	base, err := remoteRequest("x", nil, config)
	if err != nil {
		t.Fatal(err)
	}
	model := strings.Repeat("x", remoteRequestLimit-len(base)+1)
	request, err = remoteRequest(model, nil, config)
	if err != nil || len(request) != remoteRequestLimit {
		t.Fatal("exact request limit rejected")
	}
	if _, err = remoteRequest(model+"x", nil, config); err == nil {
		t.Fatal("request limit+1 accepted")
	}
}

type remoteGrowingReader struct {
	reader io.Reader
	before func()
	read   int
}

func (r *remoteGrowingReader) Read(p []byte) (int, error) {
	if r.before != nil {
		r.before()
		r.before = nil
	}
	n, err := r.reader.Read(p)
	r.read += n
	return n, err
}

func TestRemoteConcurrentResourceGrowthBounded(t *testing.T) {
	for _, size := range []int64{0, remoteResourceLimit + 4096} {
		file, err := os.Create(filepath.Join(t.TempDir(), "synthetic.jpg"))
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		if err = file.Truncate(1); err != nil {
			t.Fatal(err)
		}
		info, err := file.Stat()
		if err != nil {
			t.Fatal(err)
		}
		reader := &remoteGrowingReader{reader: file, before: func() {
			if err := file.Truncate(size); err != nil {
				t.Fatal(err)
			}
		}}
		if _, err := readRemoteResourceBytes(reader, info.Size()); err == nil {
			t.Fatal("concurrent resource size change accepted")
		}
		if reader.read > remoteResourceLimit+1 {
			t.Fatal("growth read exceeded bound")
		}
	}
}

func TestRemoteChunkedResponseBounds(t *testing.T) {
	base := remoteEnvelope(remoteTestPayload, "stop")
	for _, size := range []int{remoteResponseLimit, remoteResponseLimit + 1} {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.(http.Flusher).Flush()
			_, _ = io.WriteString(w, base+strings.Repeat(" ", size-len(base)))
		}))
		client := newRemoteClient()
		transport := client.Transport.(*http.Transport)
		transport.TLSClientConfig = server.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
		client.Transport = syntheticRoundTripper(func(r *http.Request) (*http.Response, error) {
			response, err := transport.RoundTrip(r)
			if err == nil && (len(response.TransferEncoding) != 1 || response.TransferEncoding[0] != "chunked") {
				t.Error("fixture did not use chunked wire encoding")
			}
			return response, err
		})
		endpoint, _ := normalizeRemoteURL(server.URL)
		out := callRemote(context.Background(), client, endpoint, "fixture-bearer", nil)
		server.Close()
		if (out.success != nil) != (size == remoteResponseLimit) {
			t.Fatalf("response bound %d: %+v", size, out)
		}
	}
}

func TestRemoteSelectionInputValidation(t *testing.T) {
	for _, kind := range []string{"duplicate", "unknown", "deleted", "oversize", "too-many", "not-array", "null"} {
		t.Run(kind, func(t *testing.T) {
			f := newRemoteFixture(t)
			var data []byte
			switch kind {
			case "duplicate":
				data, _ = json.Marshal([]string{f.ids[0], f.ids[0]})
			case "unknown":
				data = []byte(`["unknown"]`)
			case "deleted":
				data, _ = json.Marshal(f.ids)
				remoteFixtureSQL(t, f, func(db *sql.DB) {
					if _, err := db.Exec(`update asset set deleted_at='deleted' where id=?`, f.ids[0]); err != nil {
						t.Fatal(err)
					}
				})
			case "oversize":
				data = []byte(strings.Repeat(" ", remotePlanLimit+1))
			case "too-many":
				ids := make([]string, 1001)
				for i := range ids {
					ids[i] = "id" + strings.Repeat("x", i)
				}
				data, _ = json.Marshal(ids)
			case "not-array":
				data = []byte(`{}`)
			case "null":
				data = []byte(`[null]`)
			}
			if err := os.WriteFile(f.options.RemoteModel.AssetIDsFile, data, 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := Classify(context.Background(), f.paths, f.options); err == nil {
				t.Fatal("invalid selection accepted")
			}
			if _, err := os.Stat(f.plan); !os.IsNotExist(err) {
				t.Fatal("invalid selection wrote plan")
			}
		})
	}
}
