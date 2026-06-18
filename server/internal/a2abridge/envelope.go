package a2abridge

import (
	"encoding/json"
	"fmt"

	"github.com/a2aproject/a2a-go/v2/a2a"
)

// EnvelopeVersion is the current wire-format version. Receivers must accept
// envelopes whose version matches; envelopes with a different version are
// rejected at Unmarshal so the bridge can roll forward without silently
// double-interpreting old payloads.
const EnvelopeVersion = "1"

// EnvelopeKind discriminates the body kind of an Envelope.
type EnvelopeKind string

const (
	// KindRequest carries an A2A SendMessageRequest from the bridge to the
	// aether agent that will execute it.
	KindRequest EnvelopeKind = "request"

	// KindEvent carries a single A2A streaming event (Message, Task,
	// TaskStatusUpdateEvent, or TaskArtifactUpdateEvent) in the reverse
	// direction.
	KindEvent EnvelopeKind = "event"

	// KindError carries a bridge-level error that prevented translation or
	// delivery (auth failure, unknown agent, malformed payload, etc.).
	// Protocol-level failures (a2a TaskStateFailed) ride inside an Event
	// envelope, not here.
	KindError EnvelopeKind = "error"
)

// Envelope wraps an A2A request or event for transit through an aether
// Message.payload. The kind discriminator tells the receiver which inner
// field is populated.
//
// The envelope is JSON because it has to traverse aether's opaque payload
// bytes and the a2a-go types already serialise as JSON. Using the upstream
// SDK's own MarshalJSON for Event (via a2a.StreamResponse) keeps the wire
// format aligned with what an A2A client would observe on the network.
type Envelope struct {
	// Version pins the envelope schema. Always EnvelopeVersion for newly
	// produced envelopes.
	Version string `json:"v"`

	// Kind selects which inner field is populated.
	Kind EnvelopeKind `json:"kind"`

	// ReqID correlates a request with the stream of events it produces. The
	// gateway mints it when sending a request and stamps every replied event
	// with the same value so the sender can demux concurrent in-flight
	// requests on a single peer connection.
	ReqID string `json:"reqId,omitempty"`

	// Request is populated for KindRequest.
	Request *a2a.SendMessageRequest `json:"request,omitempty"`

	// Event is populated for KindEvent. a2a.StreamResponse's MarshalJSON
	// emits one of {"message":..., "task":..., "statusUpdate":...,
	// "artifactUpdate":...} so the wire shape mirrors the A2A streaming
	// transport.
	Event *a2a.StreamResponse `json:"event,omitempty"`

	// Terminal signals that no further events for this ReqID will arrive
	// from this peer. The bridge sets it on the last KindEvent (or alongside
	// a KindError) so the receiver can release any per-request state without
	// waiting for an a2a-protocol terminal task state.
	Terminal bool `json:"terminal,omitempty"`

	// Error is populated for KindError and describes a transport- or
	// translation-level failure.
	Error *EnvelopeError `json:"error,omitempty"`
}

// EnvelopeError describes a bridge-level failure.
type EnvelopeError struct {
	// Code is a stable machine-readable identifier (e.g. "unknown_agent",
	// "translation_failed", "internal").
	Code string `json:"code"`

	// Message is a human-readable description; safe to surface to clients.
	Message string `json:"message"`
}

// Error implements the error interface so callers can return *EnvelopeError
// directly from translation paths.
func (e *EnvelopeError) Error() string {
	if e == nil {
		return "<nil envelope error>"
	}
	return fmt.Sprintf("a2abridge: %s: %s", e.Code, e.Message)
}

// MarshalRequest produces a request envelope payload. reqID must be non-empty
// so the receiver can correlate replies; req must be non-nil.
func MarshalRequest(reqID string, req *a2a.SendMessageRequest) ([]byte, error) {
	if reqID == "" {
		return nil, fmt.Errorf("a2abridge: MarshalRequest: empty reqID")
	}
	if req == nil {
		return nil, fmt.Errorf("a2abridge: MarshalRequest: nil request")
	}
	env := Envelope{
		Version: EnvelopeVersion,
		Kind:    KindRequest,
		ReqID:   reqID,
		Request: req,
	}
	return json.Marshal(env)
}

// MarshalEvent produces an event envelope payload. evt must satisfy
// a2a.Event. terminal indicates whether this is the last event the sender
// will emit for reqID.
func MarshalEvent(reqID string, evt a2a.Event, terminal bool) ([]byte, error) {
	if reqID == "" {
		return nil, fmt.Errorf("a2abridge: MarshalEvent: empty reqID")
	}
	if evt == nil {
		return nil, fmt.Errorf("a2abridge: MarshalEvent: nil event")
	}
	env := Envelope{
		Version:  EnvelopeVersion,
		Kind:     KindEvent,
		ReqID:    reqID,
		Event:    &a2a.StreamResponse{Event: evt},
		Terminal: terminal,
	}
	return json.Marshal(env)
}

// MarshalError produces an error envelope payload. The error is always
// terminal: there is no recovery for the request beyond returning the error.
func MarshalError(reqID string, code, message string) ([]byte, error) {
	if reqID == "" {
		return nil, fmt.Errorf("a2abridge: MarshalError: empty reqID")
	}
	if code == "" {
		return nil, fmt.Errorf("a2abridge: MarshalError: empty code")
	}
	env := Envelope{
		Version:  EnvelopeVersion,
		Kind:     KindError,
		ReqID:    reqID,
		Terminal: true,
		Error:    &EnvelopeError{Code: code, Message: message},
	}
	return json.Marshal(env)
}

// Unmarshal decodes any envelope payload. It validates the version and the
// presence of the inner field corresponding to Kind. Callers should dispatch
// on Envelope.Kind.
func Unmarshal(data []byte) (*Envelope, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("a2abridge: Unmarshal: empty payload")
	}
	var env Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, fmt.Errorf("a2abridge: Unmarshal: %w", err)
	}
	if env.Version != EnvelopeVersion {
		return nil, fmt.Errorf("a2abridge: unsupported envelope version %q (want %q)", env.Version, EnvelopeVersion)
	}
	switch env.Kind {
	case KindRequest:
		if env.Request == nil {
			return nil, fmt.Errorf("a2abridge: request envelope missing request body")
		}
	case KindEvent:
		if env.Event == nil || env.Event.Event == nil {
			return nil, fmt.Errorf("a2abridge: event envelope missing event body")
		}
	case KindError:
		if env.Error == nil {
			return nil, fmt.Errorf("a2abridge: error envelope missing error body")
		}
	default:
		return nil, fmt.Errorf("a2abridge: unknown envelope kind %q", env.Kind)
	}
	return &env, nil
}
