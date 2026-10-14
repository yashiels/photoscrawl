//go:build darwin || linux

package archive

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

func TestRemoteAncestorRetainedPathRejectsReplacementAndModeDrift(t *testing.T) {
	for _, kind := range []string{"replacement", "permissions"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			intermediate := filepath.Join(root, "safe")
			private := filepath.Join(intermediate, "private")
			if err := os.MkdirAll(private, 0700); err != nil {
				t.Fatal(err)
			}
			parent, _, err := privateRemoteParent(filepath.Join(private, "plan.json"))
			if err != nil {
				t.Fatal(err)
			}
			defer parent.Close()
			if err = parent.validate(); err != nil {
				t.Fatal("safe native root ancestry rejected")
			}
			if kind == "replacement" {
				if err = os.Rename(intermediate, intermediate+"-old"); err != nil {
					t.Fatal(err)
				}
				if err = os.MkdirAll(private, 0700); err != nil {
					t.Fatal(err)
				}
			} else {
				if err = os.Chmod(intermediate, 0777); err != nil {
					t.Fatal(err)
				}
			}
			if err = parent.validate(); err == nil {
				t.Fatal("retained ancestry drift accepted")
			}
		})
	}
}

func TestRemoteAncestorNonOwnerACLRejected(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("native macOS ACL fixture")
	}
	root := t.TempDir()
	intermediate := filepath.Join(root, "safe")
	private := filepath.Join(intermediate, "private")
	if err := os.MkdirAll(private, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(intermediate, 0755); err != nil {
		t.Fatal(err)
	}
	if output, err := exec.Command("/bin/chmod", "+a", "everyone allow read", intermediate).CombinedOutput(); err != nil {
		t.Fatalf("synthetic ACL setup: %v %s", err, output)
	}
	path := filepath.Join(private, "plan.json")
	if err := writePrivateRemoteFile(path, []byte("synthetic")); err == nil {
		t.Fatal("non-owner ancestor ACL accepted")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("ACL refusal published a manifest")
	}
	if output, err := exec.Command("/bin/chmod", "-N", intermediate).CombinedOutput(); err != nil {
		t.Fatalf("synthetic ACL cleanup: %v %s", err, output)
	}
	if err := writePrivateRemoteFile(path, []byte("synthetic")); err != nil {
		t.Fatal("safe ancestor not restored")
	}
}

func TestRemoteAncestorUnknownOwnerRejected(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("unknown-owner fixture requires container root")
	}
	root := t.TempDir()
	intermediate := filepath.Join(root, "unknown")
	private := filepath.Join(intermediate, "private")
	if err := os.MkdirAll(private, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chown(intermediate, 1337, -1); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Chown(intermediate, os.Geteuid(), -1); err != nil {
			t.Error(err)
		}
	})
	if err := writePrivateRemoteFile(filepath.Join(private, "plan.json"), []byte("synthetic")); err == nil {
		t.Fatal("unknown owner accepted")
	}
}

func TestRemoteAncestorFailurePrecedesExecutionKeyAndHTTP(t *testing.T) {
	t.Setenv("PHOTOSCRAWL_MODEL_API_KEY", "invalid key with spaces")
	f := newRemoteFixture(t)
	f.planNow(t)
	root := t.TempDir()
	intermediate := filepath.Join(root, "unsafe")
	private := filepath.Join(intermediate, "private")
	if err := os.MkdirAll(private, 0700); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(f.plan)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(private, "plan.json")
	if err = os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.Chmod(intermediate, 0777); err != nil {
		t.Fatal(err)
	}
	calls := 0
	opts := f.executeOptions(fixtureRemoteClient(t, remoteEnvelope(remoteTestPayload, "stop"), 200, &calls))
	opts.RemoteModel.Plan = path
	if _, err := Classify(context.Background(), f.paths, opts); err == nil || err.Error() != "unsafe-private-file" || calls != 0 {
		t.Fatalf("unsafe ancestry reached key or HTTP: %v", err)
	}
}
