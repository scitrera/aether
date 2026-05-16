// Package aether service client implementation.
//
// This file provides the ServiceClient type for connecting to the Aether
// gateway as a Service principal. Services are HTTP/RPC endpoints addressed
// at sv::{implementation}::{specifier} that receive proxied HTTP requests
// (and, in later phases, tunnel envelopes) routed through the gateway.

package aether

import (
	pb "github.com/scitrera/aether/api/proto"
)

// ServiceClient is a client for connecting to the Aether gateway as a service
// principal. Services are identified by implementation and specifier (no
// workspace), letting them serve requests routed at the canonical
// sv::{impl}::{spec} address or the wildcard sv::{impl} form.
//
// ServiceClient embeds BaseClient and delegates all messaging to it. Service
// runtimes typically dispatch on the embedded BaseClient's OnMessage callback
// to handle inbound requests.
type ServiceClient struct {
	*BaseClient

	implementation string
	specifier      string
	credentials    map[string]string
}

// ServiceOptions configures a ServiceClient.
type ServiceOptions struct {
	ClientOptions

	// Implementation is the service implementation type (e.g. "memorylayer").
	Implementation string

	// Specifier is the service instance identifier.
	Specifier string
}

// Validate checks that required fields are set.
func (o *ServiceOptions) Validate() error {
	if o.ServerAddr == "" {
		return NewInvalidArgumentError("server address is required", "ServerAddr")
	}
	if o.Implementation == "" {
		return NewInvalidArgumentError("implementation is required", "Implementation")
	}
	if o.Specifier == "" {
		return NewInvalidArgumentError("specifier is required", "Specifier")
	}
	return nil
}

// NewServiceClient creates a new ServiceClient with the given options. The
// client is created but not connected; call Connect() to establish the
// connection.
func NewServiceClient(opts ServiceOptions) (*ServiceClient, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	cfg := BaseClientConfig{
		ServerAddr:  opts.ServerAddr,
		Connection:  opts.Connection,
		TLS:         opts.TLS,
		Credentials: opts.Credentials,
	}

	base, err := NewBaseClient(cfg)
	if err != nil {
		return nil, err
	}

	sc := &ServiceClient{
		BaseClient:     base,
		implementation: opts.Implementation,
		specifier:      opts.Specifier,
		credentials:    opts.Credentials,
	}

	base.initMsgBuilder = sc.buildInitMessage
	return sc, nil
}

// buildInitMessage creates the InitConnection message for service identity.
func (c *ServiceClient) buildInitMessage() *pb.InitConnection {
	return &pb.InitConnection{
		ClientType: &pb.InitConnection_Service{
			Service: &pb.ServiceIdentity{
				Implementation: c.implementation,
				Specifier:      c.specifier,
			},
		},
		Credentials: c.credentials,
	}
}

// Implementation returns the service's implementation type.
func (c *ServiceClient) Implementation() string {
	return c.implementation
}

// Specifier returns the service's specifier.
func (c *ServiceClient) Specifier() string {
	return c.specifier
}
