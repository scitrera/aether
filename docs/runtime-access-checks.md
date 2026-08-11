# Runtime logical-resource access checks

Aether's streaming API can authorize exact logical resources without turning
every resource protocol into an Aether-specific payload. The same primitive can
gate a workspace execution view, a tool-catalog provider, a provider-qualified
tool entry, or another application-defined resource.

This is enforcement, not the administrative `EXPLAIN_ACCESS` operation.
Every evaluation uses the ordinary ACL engine, emits the ordinary ACL audit
decision, and returns a short-lived gateway-authored receipt.

## Resource request

`ResourceAccessRequest` contains:

| Field | Meaning |
|---|---|
| `resource_type` | ACL resource family |
| `resource_id` | Exact stable logical resource ID |
| `operation` | Requested verb |
| `workspace` | Optional logical workspace scope |
| `required_access_level` | One of `10`, `20`, `30`, `40`, or `50` |
| `correlation_id` | Caller-generated binding to one logical action |

All required strings must be non-empty, canonical (no surrounding
whitespace), and within the protocol limits. Batch requests contain 1-100
items, preserve input order, and are fully validated before any item is
evaluated.

The shared resource families currently defined by Aether are:

- `workspace-execution/view`
- `tool-catalog/provider`
- `tool-catalog/entry`

Applications may use other resource families supported by their ACL policy.

## Direct and on-behalf-of checks

Without an `AuthorizationContext`, Aether evaluates the connected actor. With a
validated `on_behalf_of` context, Aether evaluates the subject's live ACL
intersected with the authority grant's access, workspace, resource, operation,
audience, and expiry constraints.

An OBO exact-resource check never falls back to the actor's own resource
permission. The grant is the delegate's authority to ask for the subject
decision; it is not a reason to silently substitute the actor after a denial.

## Decision receipts

`AccessDecisionReceipt` records the normalized request, decision and effective
level, actor, optional subject and grant lineage, evaluation/expiry times, a
stable denial code, and a unique decision ID. A checked send also binds the
receipt to the concrete post-wildcard-resolution `delivery_target`.

Receipts are trusted only when they arrive as Aether transport metadata on
`IncomingMessage.access_receipt`. An equivalent object embedded in an opaque
application payload is untrusted. Before acting, a recipient should compare
the receipt's correlation ID, resource tuple, delivery target, and expiry with
the application request it is processing.

Default receipt lifetime is 30 seconds. A `workspace-execution/view` `bind`
receipt lasts 120 seconds. Lifetimes are capped at five minutes and clamped to
the authority grant expiry.

## Checked sends

Set `SendMessage.checked_access` to make exact-resource authorization additive
to normal topic-route authorization. Aether first authorizes the concrete
route, then evaluates the exact logical resource:

- allow: publish with a gateway-authored receipt;
- deny, invalid request, or unavailable ACL service: publish nothing;
- omitted `checked_access`: retain ordinary message behavior.

The Go SDK exposes this through `SendMessageOptions.CheckedAccess`; Python has
`send_checked_message`; TypeScript accepts `checkedAccess` on
`OutgoingMessage`.

## SDK examples

Go:

```go
request := &pb.ResourceAccessRequest{
    ResourceType:        "tool-catalog/entry",
    ResourceId:          "provider-1/tool-1",
    Operation:           "invoke",
    Workspace:           "workspace-1",
    RequiredAccessLevel: 20,
    CorrelationId:       "call-1",
}
receipt, err := client.CheckAccess(ctx, request, authorization)
decisions, err := client.BatchCheckAccess(ctx, []*pb.ResourceAccessRequest{request}, authorization)
```

Python:

```python
request = aether_pb2.ResourceAccessRequest(
    resource_type="tool-catalog/entry",
    resource_id="provider-1/tool-1",
    operation="invoke",
    workspace="workspace-1",
    required_access_level=20,
    correlation_id="call-1",
)
receipt = client.check_access(request, authorization=authorization)
receipts = client.batch_check_access([request], authorization=authorization)
```

TypeScript:

```ts
const request = {
  resourceType: "tool-catalog/entry",
  resourceId: "provider-1/tool-1",
  operation: "invoke",
  workspace: "workspace-1",
  requiredAccessLevel: 20,
  correlationId: "call-1",
};
const receipt = await client.checkAccess(request, authorization);
const receipts = await client.batchCheckAccess([request], authorization);
```
