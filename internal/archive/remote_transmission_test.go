package archive

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRemoteTransmissionPreSendFailureAudit(t *testing.T) {
	t.Setenv("PHOTOSCRAWL_MODEL_API_KEY", "fixture-bearer")
	for _, kind := range []string{"dns", "dial", "tls-connect", "certificate"} {
		t.Run(kind, func(t *testing.T) {
			f := newRemoteFixture(t)
			client := newRemoteClient()
			defer client.CloseIdleConnections()
			transport := client.Transport.(*http.Transport)
			var calls atomic.Int32
			var received atomic.Int32
			switch kind {
			case "dns", "dial":
				transport.DialContext = func(context.Context, string, string) (net.Conn, error) {
					calls.Add(1)
					if kind == "dns" {
						return nil, &net.DNSError{Err: "private-transport-sentinel", Name: "private-transport-sentinel", IsNotFound: true}
					}
					return nil, errors.New("private-transport-sentinel")
				}
			case "tls-connect":
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				defer listener.Close()
				go func() {
					conn, err := listener.Accept()
					if err == nil {
						calls.Add(1)
						conn.Close()
					}
				}()
				f.options.RemoteModel.URL = "https://" + listener.Addr().String()
			case "certificate":
				server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					received.Add(1)
				}))
				server.Config.ErrorLog = log.New(io.Discard, "", 0)
				server.StartTLS()
				defer server.Close()
				f.options.RemoteModel.URL = server.URL
			}
			f.planNow(t)
			result, err := Classify(context.Background(), f.paths, f.executeOptions(client))
			if err != nil || result.ContentClassificationFailures != 1 || result.Remote.Attempts != 1 || result.Remote.Responses != 0 {
				t.Fatalf("pre-send failure counts: %+v %v", result, err)
			}
			if received.Load() != 0 || kind != "certificate" && calls.Load() != 1 {
				t.Fatal("pre-send fixture did not refuse before HTTP")
			}
			assertRemoteTransmissionAudit(t, f, result, nil, 0, 1, 0, "transport-failed")
		})
	}
}

func TestRemoteTransmissionRealTLSOutcomes(t *testing.T) {
	t.Setenv("PHOTOSCRAWL_MODEL_API_KEY", "fixture-bearer")
	for _, kind := range []string{"success", "http2-success", "response-drop", "response-read-drop", "cancel", "partial-write", "storage-failure"} {
		t.Run(kind, func(t *testing.T) {
			f := newRemoteFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			wrote := make(chan struct{})
			var once sync.Once
			ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{WroteRequest: func(info httptrace.WroteRequestInfo) {
				if info.Err == nil {
					once.Do(func() { close(wrote) })
				}
			}})
			type receipt struct {
				bytes int
				full  bool
				proto int
			}
			received := make(chan receipt, 1)
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				data, err := io.ReadAll(r.Body)
				received <- receipt{bytes: len(data), full: err == nil && int64(len(data)) == r.ContentLength, proto: r.ProtoMajor}
				if kind == "partial-write" {
					return
				}
				select {
				case <-wrote:
				case <-r.Context().Done():
					return
				}
				switch kind {
				case "cancel":
					cancel()
					<-r.Context().Done()
				case "response-drop", "response-read-drop":
					if kind == "response-read-drop" {
						w.Header().Set("Content-Length", "100")
						_, _ = io.WriteString(w, "{")
						w.(http.Flusher).Flush()
					}
					conn, _, err := w.(http.Hijacker).Hijack()
					if err == nil {
						conn.Close()
					}
				default:
					_, _ = io.WriteString(w, remoteEnvelope(remoteTestPayload, "stop"))
				}
			}))
			server.EnableHTTP2 = kind == "http2-success"
			server.StartTLS()
			defer server.Close()
			f.options.RemoteModel.URL = server.URL
			f.planNow(t)
			if kind == "storage-failure" {
				remoteFixtureSQL(t, f, func(db *sql.DB) {
					if _, err := db.Exec(`create trigger reject_transmission_storage before insert on model_observation when new.source='remote_multimodal' begin select raise(abort,'private-transport-sentinel'); end`); err != nil {
						t.Fatal(err)
					}
				})
			}
			client := server.Client()
			client.Timeout = 5 * time.Second
			if kind == "partial-write" {
				transport := client.Transport
				client.Transport = syntheticRoundTripper(func(r *http.Request) (*http.Response, error) {
					r.Body = io.NopCloser(io.MultiReader(io.LimitReader(r.Body, r.ContentLength-1), remoteTransmissionErrorReader{}))
					return transport.RoundTrip(r)
				})
			}
			defer client.CloseIdleConnections()
			result, err := Classify(ctx, f.paths, f.executeOptions(client))
			if kind == "cancel" || kind == "storage-failure" {
				wantError, code, responses, confirmed := "remote-cancelled", "cancelled", 0, false
				if kind == "storage-failure" {
					wantError, code, responses, confirmed = "remote-storage-failed", "storage-failed", 1, true
				}
				if err == nil || err.Error() != wantError {
					t.Fatalf("finalization outcome: %v", err)
				}
				assertRemoteFinalizedTransmission(t, f, result, code, responses, confirmed)
			} else {
				if err != nil || result.Remote.Attempts != 1 {
					t.Fatalf("real TLS outcome: %+v %v", result, err)
				}
				code, responses := "success", 1
				var transmitted any = true
				confirmed, unknown := 1, 0
				switch kind {
				case "response-drop":
					code, responses, transmitted, confirmed, unknown = "transport-failed", 0, nil, 0, 1
				case "response-read-drop":
					code = "response-read-failed"
				case "partial-write":
					code, responses, transmitted, confirmed, unknown = "transport-failed", 0, nil, 0, 1
				}
				assertRemoteTransmissionAudit(t, f, result, transmitted, confirmed, unknown, responses, code)
			}
			select {
			case got := <-received:
				if got.bytes == 0 || got.full != (kind != "partial-write") {
					t.Fatalf("actual TLS body receipt: %+v", got)
				}
				if kind == "http2-success" && got.proto != 2 {
					t.Fatal("HTTP/2 confirmation fixture did not negotiate HTTP/2")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("TLS body receipt was not observed")
			}
		})
	}
}

type remoteTransmissionErrorReader struct{}

func (remoteTransmissionErrorReader) Read([]byte) (int, error) {
	return 0, errors.New("private-transport-sentinel")
}

func assertRemoteFinalizedTransmission(t *testing.T, f remoteFixture, result ClassifyResult, code string, responses int, confirmed bool) {
	t.Helper()
	writes, unknown := 0, 1
	var transmitted any
	if confirmed {
		writes, unknown, transmitted = 1, 0, true
	}
	encoded, err := json.Marshal(result.Remote)
	if err != nil {
		t.Fatal(err)
	}
	var aggregate map[string]any
	if err := json.Unmarshal(encoded, &aggregate); err != nil {
		t.Fatal(err)
	}
	if aggregate["cloud_transmitted"] != transmitted || result.Remote.ConfirmedPayloadWrites != writes || result.Remote.UnknownTransmissionAttempts != unknown || result.Remote.Attempts != 1 || result.Remote.Responses != responses {
		t.Fatalf("finalization misstated transmission evidence: %+v", result.Remote)
	}
	remoteFixtureSQL(t, f, func(db *sql.DB) {
		var value string
		if err := db.QueryRow(`select metadata_json from model_run where source=?`, remoteSource).Scan(&value); err != nil {
			t.Fatal(err)
		}
		var metadata map[string]any
		if err := json.Unmarshal([]byte(value), &metadata); err != nil {
			t.Fatal(err)
		}
		if metadata["cloud_transmitted"] != transmitted || metadata["confirmed_payload_writes"] != float64(writes) || metadata["unknown_transmission_attempts"] != float64(unknown) || metadata["audit_complete"] != true || metadata["pending_attempts"] != float64(0) || metadata["confirmed_request_attempts"] != float64(1) || !strings.Contains(value, `"code":"`+code+`"`) {
			t.Fatal("finalization audit misstated local write confirmation")
		}
		if strings.Contains(value, "private-transport-sentinel") || strings.Contains(value, "raw_response") {
			t.Fatal("failed finalization stored private response or database error")
		}
	})
}

func TestRemoteTransmissionResponseWithoutWriteEvidenceIsUnknown(t *testing.T) {
	t.Setenv("PHOTOSCRAWL_MODEL_API_KEY", "fixture-bearer")
	f := newRemoteFixture(t)
	f.planNow(t)
	calls := 0
	result, err := Classify(context.Background(), f.paths, f.executeOptions(fixtureRemoteClient(t, remoteEnvelope(remoteTestPayload, "stop"), 200, &calls)))
	if err != nil || calls != 1 || result.ContentClassified != 1 {
		t.Fatalf("synthetic response: %+v %v", result, err)
	}
	assertRemoteTransmissionAudit(t, f, result, nil, 0, 1, 1, "success")
}

func TestRemoteTransmissionMixedRunRetainsUnknown(t *testing.T) {
	t.Setenv("PHOTOSCRAWL_MODEL_API_KEY", "fixture-bearer")
	f := newRemoteFixture(t)
	wrote := make(chan struct{})
	ctx := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{WroteRequest: func(info httptrace.WroteRequestInfo) {
		if info.Err == nil {
			close(wrote)
		}
	}})
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case <-wrote:
		case <-r.Context().Done():
			return
		}
		_, _ = io.WriteString(w, remoteEnvelope(remoteTestPayload, "stop"))
	}))
	defer server.Close()
	f.options.RemoteModel.URL = server.URL
	remoteFixtureSQL(t, f, func(db *sql.DB) {
		var third string
		if err := db.QueryRow(`select id from asset where local_identifier='synthetic-unrelated'`).Scan(&third); err != nil {
			t.Fatal(err)
		}
		f.ids = append(f.ids, third)
	})
	encoded, err := json.Marshal(f.ids)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(f.options.RemoteModel.AssetIDsFile, encoded, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Classify(context.Background(), f.paths, f.options); err != nil {
		t.Fatal(err)
	}
	client := server.Client()
	defer client.CloseIdleConnections()
	transport := client.Transport
	calls := 0
	client.Transport = syntheticRoundTripper(func(r *http.Request) (*http.Response, error) {
		calls++
		if calls == 1 {
			return transport.RoundTrip(r)
		}
		remoteFixtureSQL(t, f, func(db *sql.DB) {
			var value string
			if err := db.QueryRow(`select metadata_json from model_run where source=?`, remoteSource).Scan(&value); err != nil {
				t.Fatal(err)
			}
			var metadata map[string]any
			if err := json.Unmarshal([]byte(value), &metadata); err != nil {
				t.Fatal(err)
			}
			if metadata["cloud_transmitted"] != nil || metadata["confirmed_payload_writes"] != float64(1) || metadata["unknown_transmission_attempts"] != float64(1) || metadata["pending_attempts"] != float64(1) || metadata["confirmed_request_attempts"] != float64(1) || metadata["audit_complete"] != false {
				t.Fatal("pending attempt hid mixed-run uncertainty or counted an uninvoked request")
			}
		})
		return nil, errors.New("private-transport-sentinel")
	})
	result, err := Classify(ctx, f.paths, f.executeOptions(client))
	if err != nil || calls != 2 || result.ContentClassified != 1 || result.ContentClassificationFailures != 1 || result.Remote.CloudTransmitted != nil || result.Remote.ConfirmedPayloadWrites != 1 || result.Remote.UnknownTransmissionAttempts != 1 || result.Remote.Attempts != 2 || result.Remote.Responses != 1 {
		t.Fatalf("mixed run suppressed unknown or inflated confirmations: %+v %v", result.Remote, err)
	}
	remoteFixtureSQL(t, f, func(db *sql.DB) {
		var value string
		if err := db.QueryRow(`select metadata_json from model_run where source=?`, remoteSource).Scan(&value); err != nil {
			t.Fatal(err)
		}
		var metadata map[string]any
		if err := json.Unmarshal([]byte(value), &metadata); err != nil {
			t.Fatal(err)
		}
		if metadata["cloud_transmitted"] != nil || metadata["upload_may_have_started"] != true || metadata["confirmed_payload_writes"] != float64(1) || metadata["unknown_transmission_attempts"] != float64(1) || metadata["confirmed_request_attempts"] != float64(2) || metadata["pending_attempts"] != float64(0) || metadata["audit_complete"] != true {
			t.Fatal("mixed run audit did not preserve separate knowledge counts")
		}
	})
}

func assertRemoteTransmissionAudit(t *testing.T, f remoteFixture, result ClassifyResult, transmitted any, confirmed, unknown, responses int, code string) {
	t.Helper()
	check := func(value string, run bool) {
		t.Helper()
		var metadata map[string]any
		if err := json.Unmarshal([]byte(value), &metadata); err != nil {
			t.Fatal(err)
		}
		got, present := metadata["cloud_transmitted"]
		if !present || got != transmitted {
			t.Fatalf("transmission audit: cloud_transmitted=%v want=%v", got, transmitted)
		}
		if metadata["upload_may_have_started"] != true || metadata["confirmed_payload_writes"] != float64(confirmed) || metadata["unknown_transmission_attempts"] != float64(unknown) {
			t.Fatalf("transmission knowledge/counts lost: %s", value)
		}
		if run && (metadata["audit_complete"] != true || metadata["pending_attempts"] != float64(0) || metadata["confirmed_request_attempts"] != float64(1) || metadata["request_attempts"] != float64(1)) {
			t.Fatal("finished invocation confused with pending or confirmed payload")
		}
		if strings.Contains(value, "private-transport-sentinel") || strings.Contains(value, "fixture-bearer") {
			t.Fatal("private transport/auth error leaked")
		}
	}
	remoteFixtureSQL(t, f, func(db *sql.DB) {
		var run, evidence string
		if err := db.QueryRow(`select metadata_json from model_run where source=?`, remoteSource).Scan(&run); err != nil {
			t.Fatal(err)
		}
		check(run, true)
		if err := db.QueryRow(`select value_json from evidence_ref where source=?`, remoteSource).Scan(&evidence); err != nil {
			t.Fatal(err)
		}
		check(evidence, false)
		var envelope struct {
			Outcome struct {
				Code      string `json:"code"`
				Attempts  int    `json:"request_attempts"`
				Responses int    `json:"http_responses"`
			} `json:"outcome"`
		}
		if err := json.Unmarshal([]byte(evidence), &envelope); err != nil {
			t.Fatal(err)
		}
		if envelope.Outcome.Code != code || envelope.Outcome.Attempts != 1 || envelope.Outcome.Responses != responses {
			t.Fatalf("incorrect per-request audit: %+v", envelope)
		}
	})
	encoded, err := json.Marshal(result.Remote)
	if err != nil {
		t.Fatal(err)
	}
	check(string(encoded), false)
}
