// Package aether metric builder helper.
//
// This file provides MetricBuilder, a small fluent helper for constructing
// a *pb.Metric to pass to SendMetric on AgentClient, TaskClient, or
// WorkflowEngineClient.

package aether

import pb "github.com/scitrera/aether/api/proto"

// MetricBuilder is a small fluent helper for constructing a *pb.Metric.
// All entries are interpreted as additive deltas; negative qty values
// require the `capability/metric_credit` ACL permission on the sender.
type MetricBuilder struct{ m *pb.Metric }

// NewMetric returns a MetricBuilder ready to accumulate entries.
func NewMetric() *MetricBuilder { return &MetricBuilder{m: &pb.Metric{}} }

// Trace sets the optional correlation/trace ID.
func (b *MetricBuilder) Trace(id string) *MetricBuilder { b.m.TraceId = id; return b }

// Add appends one additive entry. Empty kind is allowed.
func (b *MetricBuilder) Add(name, kind string, qty float64) *MetricBuilder {
	b.m.Entries = append(b.m.Entries, &pb.MetricEntry{Name: name, Kind: kind, Qty: qty})
	return b
}

// Tag sets a metadata key (e.g. "lifecycle"="startup").
func (b *MetricBuilder) Tag(k, v string) *MetricBuilder {
	if b.m.Metadata == nil {
		b.m.Metadata = map[string]string{}
	}
	b.m.Metadata[k] = v
	return b
}

// ClientTimestampMs sets the optional client-side timestamp.
func (b *MetricBuilder) ClientTimestampMs(ts int64) *MetricBuilder {
	b.m.ClientTimestampMs = ts
	return b
}

// Build returns the constructed Metric. The builder is reusable but the
// returned pointer is shared — clone if you intend to mutate further.
func (b *MetricBuilder) Build() *pb.Metric { return b.m }
