package archive

import (
	"os"
	"path/filepath"
	"testing"
)

func TestMain(m *testing.M) {
	if resolved, err := filepath.EvalSymlinks(os.TempDir()); err == nil {
		os.Setenv("TMPDIR", resolved)
	}
	os.Exit(m.Run())
}
