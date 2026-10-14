//go:build darwin || linux

package archive

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestRemoteFinalDirectoryRequiresPrivateMode(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "existing.json")
	payload := []byte(`{"synthetic":true}`)
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	if err := writePrivateRemoteFile(path, payload); err != nil {
		t.Fatal(err)
	}
	if data, err := readPrivateRemoteFile(path); err != nil || string(data) != string(payload) {
		t.Fatalf("private positive control: %q %v", data, err)
	}
	if err := os.Chmod(root, 0755); err != nil {
		t.Fatal(err)
	}
	if data, err := readPrivateRemoteFile(path); err == nil || err.Error() != "unsafe-private-file" || data != nil {
		t.Fatalf("0755 final directory read: %q %v", data, err)
	}
	output := filepath.Join(root, "refused.json")
	if err := writePrivateRemoteFile(output, payload); err == nil || err.Error() != "unsafe-private-file" {
		t.Fatalf("0755 final directory write: %v", err)
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 1 || entries[0].Name() != "existing.json" {
		t.Fatalf("refusal published or staged a file: %v %v", entries, err)
	}
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	if err := writePrivateRemoteFile(output, payload); err != nil {
		t.Fatal("restored private write:", err)
	}
	if data, err := readPrivateRemoteFile(output); err != nil || string(data) != string(payload) {
		t.Fatalf("restored private read: %q %v", data, err)
	}
}

func TestRemoteFinalDirectoryRefusalPrecedesKeyAndHTTP(t *testing.T) {
	t.Setenv("PHOTOSCRAWL_MODEL_API_KEY", "invalid key with spaces")
	f := newRemoteFixture(t)
	f.planNow(t)
	if err := os.Chmod(filepath.Dir(f.plan), 0755); err != nil {
		t.Fatal(err)
	}
	calls := 0
	opts := f.executeOptions(fixtureRemoteClient(t, remoteEnvelope(remoteTestPayload, "stop"), 200, &calls))
	if _, err := Classify(context.Background(), f.paths, opts); err == nil || err.Error() != "unsafe-private-file" || calls != 0 {
		t.Fatalf("0755 final directory reached key or HTTP: calls=%d err=%v", calls, err)
	}
	if err := os.Chmod(filepath.Dir(f.plan), 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := Classify(context.Background(), f.paths, opts); err == nil || err.Error() != "invalid-remote-key:PHOTOSCRAWL_MODEL_API_KEY" || calls != 0 {
		t.Fatalf("private execution positive control: calls=%d err=%v", calls, err)
	}
}

type remoteOwnershipDriftReader struct {
	file *os.File
	uid  int
	gid  int
	once bool
}

func (r *remoteOwnershipDriftReader) Read(data []byte) (int, error) {
	n, err := r.file.Read(data)
	if n > 0 && !r.once {
		r.once = true
		if changeErr := r.file.Chown(r.uid, r.gid); changeErr != nil {
			return n, changeErr
		}
	}
	return n, err
}

func TestRemotePrivateReadRejectsOwnershipDrift(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("real UID/GID drift requires container root")
	}
	for _, kind := range []string{"unchanged", "uid", "gid"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			if err := os.Chmod(root, 0700); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(root, "synthetic.json")
			payload := []byte(`{"synthetic":true}`)
			if err := writePrivateRemoteFile(path, payload); err != nil {
				t.Fatal(err)
			}
			parent, name, err := privateRemoteParent(path)
			if err != nil {
				t.Fatal(err)
			}
			defer parent.Close()
			file, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			var before unix.Stat_t
			if err := unix.Fstat(int(file.Fd()), &before); err != nil {
				t.Fatal(err)
			}
			uid, gid := -1, -1
			if kind == "uid" {
				uid = int(before.Uid) + 1
			}
			if kind == "gid" {
				gid = int(before.Gid) + 1
			}
			reader := &remoteOwnershipDriftReader{file: file, uid: uid, gid: gid}
			data, readErr := readRemotePrivateContents(parent, name, file, before, io.Reader(reader))
			var after unix.Stat_t
			if err := unix.Fstat(int(file.Fd()), &after); err != nil {
				t.Fatal(err)
			}
			if !reader.once || before.Dev != after.Dev || before.Ino != after.Ino || before.Mode != after.Mode || before.Size != after.Size {
				t.Fatal("drift fixture changed another identity field or did not run")
			}
			if kind == "unchanged" {
				if after.Uid != before.Uid || after.Gid != before.Gid || readErr != nil || string(data) != string(payload) {
					t.Fatalf("unchanged ownership positive control: %q %v", data, readErr)
				}
				return
			}
			if kind == "uid" && (after.Uid != uint32(uid) || after.Gid != before.Gid) || kind == "gid" && (after.Gid != uint32(gid) || after.Uid != before.Uid) {
				t.Fatal("requested ownership drift did not occur in isolation")
			}
			if readErr == nil || readErr.Error() != "unsafe-private-file" || data != nil {
				t.Fatalf("%s drift during bounded read accepted: data=%q err=%v", kind, data, readErr)
			}
		})
	}
}
