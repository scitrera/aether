package proxysidecar

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestInitiator_HTTPHandler_DispatchesProxyRequestToTarget(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		Gateway: GatewayConfig{
			Address:  "localhost:50051",
			Insecure: true,
		},
		Initiator: InitiatorConfig{
			Enabled: true,
			Listen:  ListenConfig{Bind: "127.0.0.1:0"},
			Target:  TargetConfig{Topic: "sv::memorylayer::default"},
		},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("config: %v", err)
	}

	init, err := NewInitiator(cfg)
	if err != nil {
		t.Fatalf("NewInitiator: %v", err)
	}

	disp := &captureDispatcher{
		respFn: func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: 202,
				Header:     http.Header{"X-Proxy-Backend": []string{"memorylayer"}},
				Body:       io.NopCloser(strings.NewReader(`{"ack":true}`)),
			}, nil
		},
	}
	init.SetDispatcher(disp)

	server := httptest.NewServer(init)
	defer server.Close()

	body := bytes.NewBufferString(`{"name":"alice"}`)
	httpReq, err := http.NewRequest(http.MethodPost, server.URL+"/v1/users?dry_run=true", body)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		t.Fatalf("call initiator: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 202 {
		t.Errorf("status: got %d, want 202", resp.StatusCode)
	}
	if got := resp.Header.Get("X-Proxy-Backend"); got != "memorylayer" {
		t.Errorf("X-Proxy-Backend echoed: got %q, want %q", got, "memorylayer")
	}

	if disp.calls != 1 {
		t.Fatalf("dispatcher called %d times, want 1", disp.calls)
	}
	if disp.target != "sv::memorylayer::default" {
		t.Errorf("dispatched target: got %q, want %q", disp.target, "sv::memorylayer::default")
	}
	if disp.req == nil {
		t.Fatal("dispatcher saw nil request")
	}
	if disp.req.Method != http.MethodPost {
		t.Errorf("dispatched method: got %q, want POST", disp.req.Method)
	}
	if disp.req.URL.Path != "/v1/users" {
		t.Errorf("dispatched path: got %q, want /v1/users", disp.req.URL.Path)
	}
	if got := disp.req.URL.RawQuery; got != "dry_run=true" {
		t.Errorf("dispatched query: got %q, want dry_run=true", got)
	}
	if got := disp.req.Header.Get("Content-Type"); got != "application/json" {
		t.Errorf("dispatched Content-Type: got %q", got)
	}
	if got := string(disp.body); got != `{"name":"alice"}` {
		t.Errorf("dispatched body: got %q", got)
	}
}

// TestEncodeProxyHttpRequest_FromHTTP_ShapeMatchesSDK asserts the
// initiator's helper produces the same envelope shape as the SDK's
// ProxyHTTP path.
func TestEncodeProxyHttpRequest_FromHTTP_ShapeMatchesSDK(t *testing.T) {
	t.Parallel()

	r := httptest.NewRequest(http.MethodPost, "http://ignored/v1/items?x=1", strings.NewReader("hi"))
	r.Header.Set("Content-Type", "text/plain")

	got := Encode_ProxyHttpRequest_FromHTTP(r, "sv::memorylayer::default", "rid-42", []byte("hi"))

	if got.GetTargetTopic() != "sv::memorylayer::default" {
		t.Errorf("TargetTopic: got %q", got.GetTargetTopic())
	}
	if got.GetMethod() != http.MethodPost {
		t.Errorf("Method: got %q", got.GetMethod())
	}
	if got.GetPath() != "/v1/items?x=1" {
		t.Errorf("Path (RequestURI): got %q", got.GetPath())
	}
	if string(got.GetBody()) != "hi" {
		t.Errorf("Body: got %q", string(got.GetBody()))
	}
	if got.GetHeaders()["Content-Type"] != "text/plain" {
		t.Errorf("Content-Type header: got %q", got.GetHeaders()["Content-Type"])
	}
	if got.GetRequestId() != "rid-42" {
		t.Errorf("RequestId: got %q", got.GetRequestId())
	}
}
