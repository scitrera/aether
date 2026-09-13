# Subject input for an active task

A user session may send an OPAQUE message to
`tk::{workspace}::{task_id}::input`. Aether checks the ordinary workspace send
permission and loads the task from its own store. The task must be running,
mark its subject as a participant, and name the authenticated user as its
authority subject. The session workspace must match the task workspace.
When `turn_tool_host_id` is present, it must match the exact user-session topic.

The gateway forwards unchanged payload bytes to the task's exact stored agent
assignee, retaining the authenticated user-session source and task workspace.
The route does not start an offline worker. Completed, missing, unassigned,
cross-subject and cross-window tasks fail closed with a generic refusal.
Service senders, delegated authorization, checked-access requests and authority
continuation are not supported by this route.

This is a task transport for client results. It does not grant user access to
an internal worker workspace. Receivers must still correlate replies with their
pending call and verify the authenticated source. Browser catalog consumers
already check the exact provider topic and call identifier. A task input send
has the same asynchronous error reporting as other SendMessage operations.
