package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRemoteCLIContractBeforeArchive(t *testing.T) {
	for _, args := range [][]string{
		{"--remote-model=vision"},
		{"--remote-model="},
		{"--remote-dry-run", "--remote-model=vision", "--remote-model-url=http://example.test", "--asset-ids-file=ids", "--remote-plan-out=plan"},
		{"--remote-plan=plan"},
		{"--remote-plan=plan", "--allow-model-upload", "--all"},
		{"--remote-plan=plan", "--allow-model-upload", "--local-model="},
		{"--remote-plan=plan", "--allow-model-upload", "--local-model-api=openai"},
		{"--remote-plan=plan", "--allow-model-upload", "--allow-icloud-downloads"},
		{"--remote-plan=plan", "--allow-model-upload", "--retry-content-failed=false"},
		{"--remote-plan=plan", "--allow-model-upload", "--remote-model=override"},
		{"--remote-plan=plan", "--allow-model-upload", "--limit=1001"},
	} {
		db := filepath.Join(t.TempDir(), "must-not-exist.sqlite")
		argv := append([]string{"classify", "--db", db}, args...)
		err := run(context.Background(), argv)
		if err == nil || strings.Contains(err.Error(), "flag provided but not defined") {
			t.Fatalf("remote flags not validated: %v", err)
		}
		if _, err := os.Stat(db); !os.IsNotExist(err) {
			t.Fatal("invalid remote configuration opened archive")
		}
	}
}
