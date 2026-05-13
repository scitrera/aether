// Package aether async-handler helpers.
//
// The receive loop dispatches each downstream message to its registered
// handler synchronously on a single goroutine (see client.go::dispatchResponse).
// Any handler that itself makes a synchronous SDK call back to the gateway —
// CreateTaskSync, KV ops with Wait*, ProxyHTTP, derive_authority_grant — will
// deadlock: the response it is waiting for can only arrive on the very loop
// the handler is holding.
//
// The helpers in this file are the recommended idiom for writing service-side
// handlers that need to make nested SDK calls. They detach handler execution
// from the receive loop by spawning a goroutine per inbound frame, restoring
// the natural request → response flow at the cost of in-order handler
// dispatch.
//
// Future work tracked in README.md (§ "Handler dispatch model"): a built-in
// async dispatch mode for OnProxyHttpRequest and OnTaskAssignment with a
// bounded worker pool.

package aether

import (
	"context"
	"log/slog"
	"time"

	pb "github.com/scitrera/aether/api/proto"
)

// DefaultAsyncHandlerTimeout caps how long a wrapped async handler is allowed
// to run before its context is canceled. Service handlers that need a
// different ceiling should call AsyncWithTimeout directly. The chosen value
// covers slow lifecycle ops (e.g., container start, K8s pod readiness) without
// leaving leaked goroutines behind on a stuck handler.
const DefaultAsyncHandlerTimeout = 3 * time.Minute

// Async wraps a synchronous ProxyHttpRequestHandler so that each inbound
// request runs on its own goroutine, freeing the receive loop to deliver
// nested response frames the handler may be waiting on.
//
// The wrapped handler is given a fresh context bounded by
// DefaultAsyncHandlerTimeout (independent of the receive loop's context),
// errors are logged at warn level instead of bubbling to the loop, and panics
// are recovered to prevent one buggy request from killing the service.
//
// Use this for any handler that needs to make synchronous SDK calls back to
// the gateway. See README.md § "Handler dispatch model" for the rationale.
//
//	client.OnProxyHttpRequest(aether.Async(func(ctx context.Context, req *pb.ProxyHttpRequest) error {
//	    // Free to call client.CreateTaskSync, KV ops, ProxyHTTP, etc.
//	    return nil
//	}))
func Async(h ProxyHttpRequestHandler) ProxyHttpRequestHandler {
	return AsyncWithTimeout(h, DefaultAsyncHandlerTimeout)
}

// AsyncWithTimeout is Async with a caller-supplied per-request ceiling. Pass
// 0 to run without a timeout (not recommended for production services).
func AsyncWithTimeout(h ProxyHttpRequestHandler, timeout time.Duration) ProxyHttpRequestHandler {
	return func(_ context.Context, req *pb.ProxyHttpRequest) error {
		go runAsyncProxyHandler(h, req, timeout)
		return nil
	}
}

// AsyncMessageHandler is the equivalent for OnMessage handlers.
func AsyncMessageHandler(h MessageHandler) MessageHandler {
	return AsyncMessageHandlerWithTimeout(h, DefaultAsyncHandlerTimeout)
}

// AsyncMessageHandlerWithTimeout is AsyncMessageHandler with a custom timeout.
func AsyncMessageHandlerWithTimeout(h MessageHandler, timeout time.Duration) MessageHandler {
	return func(_ context.Context, msg *Message) error {
		go runAsyncMessageHandler(h, msg, timeout)
		return nil
	}
}

// AsyncTaskAssignmentHandler is the equivalent for OnTaskAssignment handlers.
func AsyncTaskAssignmentHandler(h TaskAssignmentHandler) TaskAssignmentHandler {
	return AsyncTaskAssignmentHandlerWithTimeout(h, DefaultAsyncHandlerTimeout)
}

// AsyncTaskAssignmentHandlerWithTimeout is AsyncTaskAssignmentHandler with a
// custom timeout.
func AsyncTaskAssignmentHandlerWithTimeout(h TaskAssignmentHandler, timeout time.Duration) TaskAssignmentHandler {
	return func(_ context.Context, task *TaskAssignment) error {
		go runAsyncTaskAssignmentHandler(h, task, timeout)
		return nil
	}
}

// =============================================================================
// internal goroutine bodies
// =============================================================================

func runAsyncProxyHandler(h ProxyHttpRequestHandler, req *pb.ProxyHttpRequest, timeout time.Duration) {
	ctx, cancel := newAsyncContext(timeout)
	defer cancel()
	defer recoverAsyncPanic(ctx, "proxy_http_request_handler",
		slog.String("request_id", req.GetRequestId()),
		slog.String("path", req.GetPath()),
	)
	if err := h(ctx, req); err != nil {
		slog.WarnContext(ctx, "async proxy handler returned error",
			"request_id", req.GetRequestId(),
			"path", req.GetPath(),
			"err", err,
		)
	}
}

func runAsyncMessageHandler(h MessageHandler, msg *Message, timeout time.Duration) {
	ctx, cancel := newAsyncContext(timeout)
	defer cancel()
	defer recoverAsyncPanic(ctx, "message_handler",
		slog.String("source_topic", msg.SourceTopic),
	)
	if err := h(ctx, msg); err != nil {
		slog.WarnContext(ctx, "async message handler returned error",
			"source_topic", msg.SourceTopic,
			"err", err,
		)
	}
}

func runAsyncTaskAssignmentHandler(h TaskAssignmentHandler, task *TaskAssignment, timeout time.Duration) {
	ctx, cancel := newAsyncContext(timeout)
	defer cancel()
	defer recoverAsyncPanic(ctx, "task_assignment_handler",
		slog.String("task_id", task.TaskID),
	)
	if err := h(ctx, task); err != nil {
		slog.WarnContext(ctx, "async task assignment handler returned error",
			"task_id", task.TaskID,
			"err", err,
		)
	}
}

func newAsyncContext(timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return context.WithCancel(context.Background())
	}
	return context.WithTimeout(context.Background(), timeout)
}

func recoverAsyncPanic(ctx context.Context, kind string, attrs ...slog.Attr) {
	r := recover()
	if r == nil {
		return
	}
	args := []any{"kind", kind, "panic", r}
	for _, a := range attrs {
		args = append(args, a.Key, a.Value)
	}
	slog.ErrorContext(ctx, "async handler panicked", args...)
}
