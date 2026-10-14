package archive

import (
	"strings"
	"testing"
)

func TestRemoteEndpointContract(t *testing.T) {
	for input, want := range map[string]string{
		"https://example.test":                         "https://example.test/v1/chat/completions",
		"https://example.test/v1/":                     "https://example.test/v1/chat/completions",
		"https://example.test/custom/chat/completions": "https://example.test/custom/chat/completions",
	} {
		got, err := normalizeRemoteURL(input)
		if err != nil || got != want {
			t.Fatalf("normalization: %q %v", got, err)
		}
	}
	for _, input := range []string{"http://example.test", "https://user:secret@example.test", "https://example.test/?key=secret", "https://example.test/#secret", "https://example.test/custom", "https://example.test:0", "https://example.test:99999", "https://example.test/\nsecret", "https://example.test/\u0085/chat/completions", "https://example.test/%0a/chat/completions"} {
		if _, err := normalizeRemoteURL(input); err == nil || strings.Contains(err.Error(), "secret") {
			t.Fatalf("unsafe endpoint accepted or echoed")
		}
	}
}

func TestRemoteStrictJSONRejectsInvalidUTF8AndDuplicateKeys(t *testing.T) {
	for _, data := range [][]byte{[]byte("[\"\xff\"]"), []byte(`{"version":1,"version":2}`), []byte(`{} {}`)} {
		if err := strictRemoteJSON(data, new(any)); err == nil {
			t.Fatal("non-strict JSON accepted")
		}
	}
}

const remoteTestPayload = `{"scene_summary":"A synthetic scene.","visible_text_summary":null,"people_presence":"No people visible.","place_candidates":[],"landmark_candidates":[],"merchant_or_venue_candidates":[],"food_or_objects":[],"privacy_sensitivity":[],"cluster_terms":[],"uncertainties":[]}`

func TestRemoteSchemaPolicy(t *testing.T) {
	if _, err := validateRemotePayload(remoteTestPayload); err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{"return only valid compact", "Do Not Use Markdown Fences", "use candidates, not truth", "KEYS:", `\u006beys:`} {
		raw := strings.Replace(remoteTestPayload, "A synthetic scene.", fragment+" sentinel-private", 1)
		if _, err := validateRemotePayload(raw); err == nil || err.Error() != "prompt-leakage" {
			t.Fatalf("policy failure: %v", err)
		}
	}
	for _, raw := range []string{`{}`, remoteTestPayload + " trailing", strings.Replace(remoteTestPayload, `"cluster_terms":[]`, `"cluster_terms":null`, 1), strings.Replace(remoteTestPayload, `"scene_summary":"A synthetic scene."`, `"scene_summary":42`, 1), strings.Replace(remoteTestPayload, `"uncertainties":[]`, `"unknown":[]`, 1)} {
		if _, err := validateRemotePayload(raw); err == nil {
			t.Fatal("invalid schema accepted")
		}
	}
	if _, err := validateRemotePayload(strings.Replace(remoteTestPayload, "A synthetic scene.", "Keys on a table, returning compact parcels.", 1)); err != nil {
		t.Fatal("benign near-match rejected")
	}
}

func TestRemoteKeyValidation(t *testing.T) {
	for _, key := range []string{"", "has space", "tab\tkey", "line\nkey", "nonascii-é", strings.Repeat("x", 4097)} {
		t.Setenv("PHOTOSCRAWL_TEST_REMOTE_KEY", key)
		if _, err := remoteKey("PHOTOSCRAWL_TEST_REMOTE_KEY"); err == nil {
			t.Fatal("invalid key accepted")
		}
	}
	t.Setenv("PHOTOSCRAWL_TEST_REMOTE_KEY", strings.Repeat("x", 4096))
	if key, err := remoteKey("PHOTOSCRAWL_TEST_REMOTE_KEY"); err != nil || len(key) != 4096 {
		t.Fatal("exact key bound rejected")
	}
	if _, err := remoteKey("bad-name"); err == nil {
		t.Fatal("invalid environment variable name accepted")
	}
}
