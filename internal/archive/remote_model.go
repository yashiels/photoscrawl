package archive

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"

	repoPrompts "github.com/openclaw/photoscrawl/prompts"
)

const (
	remoteSource        = "remote_multimodal"
	remotePromptVersion = repoPrompts.RemoteMultimodalObservationsV1Version
	remoteResourceLimit = 16 << 20
	remoteRequestLimit  = 25 << 20
	remoteResponseLimit = 1 << 20
	remotePlanLimit     = 1 << 20
	remoteAssetLimit    = 1000
)

type RemoteModelOptions struct {
	FlagsSet           bool
	Model              string
	URL                string
	KeyEnv             string
	AssetIDsFile       string
	PlanOut            string
	Plan               string
	DryRun             bool
	AllowUpload        bool
	RetryContentFailed bool
	LocalFlagsSet      bool
	RetryFlagSet       bool
	client             *http.Client
}

func (o RemoteModelOptions) active() bool {
	return o.FlagsSet || o.Model != "" || o.URL != "" || o.KeyEnv != "" || o.AssetIDsFile != "" || o.PlanOut != "" || o.Plan != "" || o.DryRun || o.AllowUpload || o.RetryContentFailed || o.RetryFlagSet
}

func (o RemoteModelOptions) validate(opts ClassifyOptions) error {
	if opts.All || opts.AllowICloudDownloads || opts.LocalModel != "" || opts.LocalModelURL != "" || opts.LocalModelAPI != "" || o.LocalFlagsSet || opts.Limit < 0 || opts.Limit > remoteAssetLimit {
		return errors.New("remote-configuration-conflict")
	}
	if o.KeyEnv != "" && !remoteEnvName.MatchString(o.KeyEnv) {
		return errors.New("invalid-key-environment-name")
	}
	if o.DryRun {
		if o.Plan != "" || o.AssetIDsFile == "" || o.PlanOut == "" || o.AllowUpload || !validRemoteModel(o.Model) {
			return errors.New("invalid-remote-plan-configuration")
		}
		_, err := normalizeRemoteURL(o.URL)
		return err
	}
	if o.Plan == "" || !o.AllowUpload || o.AssetIDsFile != "" || o.PlanOut != "" || o.Model != "" || o.URL != "" || o.RetryContentFailed || o.RetryFlagSet {
		return errors.New("remote-plan-and-upload-consent-required")
	}
	return nil
}

var remoteEnvName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func validRemoteModel(model string) bool {
	if strings.TrimSpace(model) == "" || !utf8.ValidString(model) {
		return false
	}
	for _, r := range model {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func normalizeRemoteURL(value string) (string, error) {
	invalid := errors.New("invalid-remote-endpoint")
	for _, r := range value {
		if unicode.IsControl(r) || unicode.IsSpace(r) {
			return "", invalid
		}
	}
	u, err := url.Parse(value)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" {
		return "", invalid
	}
	if strings.Contains(value, "#") || strings.HasSuffix(u.Host, ":") {
		return "", invalid
	}
	for _, r := range u.Path {
		if unicode.IsControl(r) {
			return "", invalid
		}
	}
	if port := u.Port(); port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return "", invalid
		}
	}
	path := strings.TrimRight(u.EscapedPath(), "/")
	switch {
	case path == "":
		u.Path = "/v1/chat/completions"
		u.RawPath = ""
	case path == "/v1":
		u.Path = "/v1/chat/completions"
		u.RawPath = ""
	case strings.HasSuffix(path, "/chat/completions"):
		u.Path = strings.TrimRight(u.Path, "/")
		u.RawPath = strings.TrimRight(u.RawPath, "/")
	default:
		return "", invalid
	}
	u.Host = strings.ToLower(u.Host)
	return u.String(), nil
}

func remoteKey(name string) (string, error) {
	if name == "" {
		name = "PHOTOSCRAWL_MODEL_API_KEY"
	}
	if !remoteEnvName.MatchString(name) {
		return "", errors.New("invalid-key-environment-name")
	}
	key := os.Getenv(name)
	if len(key) == 0 || len(key) > 4096 {
		return "", errors.New("invalid-remote-key:" + name)
	}
	for _, c := range []byte(key) {
		if c < 33 || c > 126 {
			return "", errors.New("invalid-remote-key:" + name)
		}
	}
	return key, nil
}

type remoteSuccess struct {
	raw     string
	payload map[string]any
}

type remoteOutcome struct {
	Code                  string                `json:"code"`
	Status                int                   `json:"http_status,omitempty"`
	Attempts              int                   `json:"request_attempts"`
	Responses             int                   `json:"http_responses"`
	PayloadWriteConfirmed bool                  `json:"payload_write_confirmed"`
	Diagnostics           CompletionDiagnostics `json:"completion_diagnostics"`
	success               *remoteSuccess
}

func newRemoteClient() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DisableKeepAlives = true
	return &http.Client{Transport: transport, Timeout: 5 * time.Minute, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

func remoteRequest(model string, data []byte, config CompletionConfig) ([]byte, error) {
	if len(data) > remoteResourceLimit {
		return nil, errors.New("resource-oversize")
	}
	request := openAIChatCompletionRequest{
		Model: model,
		Messages: []openAIChatMessage{{Role: "user", Content: []openAIChatMessagePart{
			{Type: "text", Text: repoPrompts.RemoteMultimodalObservationsV1},
			{Type: "image_url", ImageURL: &openAIChatImageURLPart{URL: "data:" + http.DetectContentType(data) + ";base64," + base64.StdEncoding.EncodeToString(data)}},
		}}},
		Temperature: 0.1, MaxTokens: config.MaxTokens, ReasoningEffort: config.ReasoningEffort,
	}
	if config.ReasoningEnabled != nil || config.ReasoningMaxTokens != 0 {
		request.Reasoning = &openRouterReasoning{Enabled: config.ReasoningEnabled, MaxTokens: config.ReasoningMaxTokens}
	}
	body, err := json.Marshal(request)
	if err != nil || len(body) > remoteRequestLimit {
		return nil, errors.New("request-oversize")
	}
	return body, nil
}

func callRemote(ctx context.Context, client *http.Client, endpoint, key string, body []byte) (out remoteOutcome) {
	out = remoteOutcome{Code: "transport-failed"}
	var written atomic.Bool
	responseReceived := false
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{WroteRequest: func(info httptrace.WroteRequestInfo) {
		if info.Err == nil {
			written.Store(true)
		}
	}})
	defer func() { out.PayloadWriteConfirmed = responseReceived && written.Load() }()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		out.Code = "invalid-request"
		return out
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+key)
	out.Attempts = 1
	resp, err := client.Do(req)
	if err != nil {
		return out
	}
	responseReceived = true
	defer resp.Body.Close()
	out.Responses = 1
	out.Status = resp.StatusCode
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		out.Code = "http-failed"
		return out
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, remoteResponseLimit+1))
	if err != nil {
		out.Code = "response-read-failed"
		return out
	}
	if len(data) > remoteResponseLimit {
		out.Code = "response-oversize"
		return out
	}
	return decodeRemoteCompletion(data, out)
}

func decodeRemoteCompletion(data []byte, out remoteOutcome) remoteOutcome {
	out.Code = "invalid-envelope"
	if !utf8.Valid(data) || !uniqueRemoteJSON(data) {
		return out
	}
	var envelope struct {
		Error   json.RawMessage  `json:"error"`
		Usage   *CompletionUsage `json:"usage"`
		Choices []struct {
			Finish  string `json:"finish_reason"`
			Message struct {
				Content string          `json:"content"`
				Refusal json.RawMessage `json:"refusal"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return out
	}
	if u := envelope.Usage; u != nil && u.PromptTokens >= 0 && u.CompletionTokens >= 0 && u.TotalTokens >= 0 && (u.CompletionTokensDetails == nil || u.CompletionTokensDetails.ReasoningTokens >= 0) {
		out.Diagnostics.Usage = u
	}
	if len(envelope.Error) > 0 && string(envelope.Error) != "null" {
		out.Code = "provider-error"
		return out
	}
	if len(envelope.Choices) != 1 {
		out.Code = "invalid-choice-count"
		return out
	}
	c := envelope.Choices[0]
	switch c.Finish {
	case "stop", "length", "content_filter", "tool_calls", "function_call":
		out.Diagnostics.FinishReason = c.Finish
	}
	if c.Finish != "stop" {
		out.Code = "invalid-finish"
		return out
	}
	if len(c.Message.Refusal) > 0 && string(c.Message.Refusal) != "null" && string(c.Message.Refusal) != `""` {
		out.Code = "refusal"
		return out
	}
	if strings.TrimSpace(c.Message.Content) == "" {
		out.Code = "empty-content"
		return out
	}
	payload, err := validateRemotePayload(c.Message.Content)
	if err != nil {
		out.Code = err.Error()
		return out
	}
	out.Code = "success"
	out.success = &remoteSuccess{raw: c.Message.Content, payload: payload}
	return out
}

func validateRemotePayload(raw string) (map[string]any, error) {
	invalid := errors.New("invalid-schema")
	if !utf8.ValidString(raw) || !uniqueRemoteJSON([]byte(raw)) {
		return nil, invalid
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal([]byte(raw), &fields) != nil || len(fields) != 10 {
		return nil, invalid
	}
	payload := map[string]any{}
	values := []string{}
	for _, name := range []string{"scene_summary", "visible_text_summary", "people_presence", "place_candidates", "landmark_candidates", "merchant_or_venue_candidates", "food_or_objects", "privacy_sensitivity", "cluster_terms", "uncertainties"} {
		value, ok := fields[name]
		if !ok {
			return nil, invalid
		}
		if name == "visible_text_summary" && string(value) == "null" {
			payload[name] = nil
			continue
		}
		if name == "scene_summary" || name == "visible_text_summary" || name == "people_presence" {
			var text string
			if len(value) == 0 || value[0] != '"' || json.Unmarshal(value, &text) != nil || (name != "visible_text_summary" && strings.TrimSpace(text) == "") {
				return nil, invalid
			}
			payload[name] = text
			values = append(values, text)
		} else {
			var texts []string
			if len(value) == 0 || value[0] != '[' || json.Unmarshal(value, &texts) != nil {
				return nil, invalid
			}
			var elements []json.RawMessage
			if json.Unmarshal(value, &elements) != nil {
				return nil, invalid
			}
			for _, element := range elements {
				if len(element) == 0 || element[0] != '"' {
					return nil, invalid
				}
			}
			payload[name] = texts
			values = append(values, texts...)
		}
	}
	for _, value := range values {
		for _, fragment := range []string{"return only valid compact", "do not use markdown fences", "use candidates, not truth", "keys:"} {
			if strings.Contains(strings.ToLower(value), fragment) {
				return nil, errors.New("prompt-leakage")
			}
		}
	}
	return payload, nil
}
