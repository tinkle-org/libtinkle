# Architecture

```text
Swift/Kotlin bindings        native configuration, platform secret/cursor stores
        ↓
libtinkle                   typed/raw application requests, one supervisor,
                            paged history, journals, cache projection, file/push sync
        ↓
syntra-runtime              RPC correlation, deadlines, cancellation, notifications
        ↓
syntra-net (tcp only)        single-attempt encrypted native TCP, owned pumps
        ↓
syntra-core                 framing, secure channel, pinned server + Returning proof
```

`tinkle-schema` owns application payloads and validation. It is independent of
transport selection. Its artifact/dependency update is coordinated separately.
`tinkle-storage` owns application SQLite rows, journals, transfer state and
background jobs; authorization keys never enter that database.

All three Syntra crates are exact crates.io `=1.0.0-rc.3` dependencies, resolved
and tested through the ordinary Cargo workspace without local patches. The application/runtime boundary is
opaque protobuf payload bytes. Device enrollment maps the public schema's exact
messages into zeroizing native owners and secret-free status projections.

## Establishment

The caller provides bare host/ports and a typed `TrustSource`. Both ports share
the exact source. One 15-second budget covers TCP dial selection, channel proof
and Core readiness. TCP 443 is primary, TCP 80 is the dial-failure fallback.
After socket selection, no establishment error selects another port.
The selected port remains fixed through reconnects because the TCP-only SDK
does not publicly expose its private post-ready failure classification. Explicit
trust replacement authorizes fresh route selection.

`Bootstrap` and `Returning` are explicit. Returning carries a nonzero AK32,
handle8 and epoch. The key is copied only into a fresh SDK `Secret32` for each
attempt. The host owns protected enrollment storage and installs new material
using `replace_trust_source`. No automatic mode change or key persistence occurs.

The existing application supervisor schedules reconnect attempts. The SDK's
reconnecting client is not enabled. Manual `reconnect` makes one attempt and
restores retained application interests/watch sets. Requests are never replayed
by transport reconnection.

Each ACTIVE Returning generation owns one bounded five-second application
KeepAlive task. It calls the public raw-request path, so initialization and
authorization remain in that path. A generation fence prevents a retired task
from addressing a replacement socket. Failure classification precedes supervised
recovery; terminal authorization/protocol failures remain parked. Liveness
sequence state is connection-local and is never stored in application journals.
Lifecycle retirement cancels and joins the task; cancelled teardown retains its
join owner for the next lifecycle operation. Bootstrap has no KeepAlive task.

`ClientInfo` is an immutable complete snapshot, validated before construction.
Each live connection owns a separate first-call initialization gate. Its first
public unary call is wrapped in `InitConnect` under the same caller budget.
Enrollment operation/candidate/active writes use an acknowledged external secure
store; activation is a separate pinned Bootstrap request. Only a verified ACTIVE
response plus durable active installation can produce the next Returning source.

Local logout synchronously fences admission and spawns retained, joinable cleanup.
Its single remote mutation uses only the captured Returning generation. Cache and
secure-store retries preserve completed steps and the original principal.
Conditional hosts compare a durable namespace incarnation/revision, persist a
cleanup marker, and reject stale writes or clears after replacement. Legacy
hosts cannot attest restart-safe cleanup. The account lifecycle query keeps
network availability, authorization knowledge and cleanup progress distinct.

The enrollment observer uses status-first restore/foreground entry plus one
independent 60-second foreground schedule. Each status-only Bootstrap connection
shares the main selected route and public trust, with a five-second absolute
budget. Returning outcomes cannot schedule or accelerate it. Source-install and
durable namespace revisions fence stale responses, pending activation and
cross-Client writes. Stop/suspend cancel and drain host-owned observations as
well as the periodic task; foreground Returning admission stays parked until
status-first work settles. Exact retirement invokes retained local-only cleanup.
The disposition contract is published in BSR prost artifact
`0.5.0-20261107080548-dc41c2199c76.2` following schema #57. Release qualification
uses the published Cargo pin and ordinary `--locked` workspace checks; required
CI needs neither Buf nor schema-candidate generation.

## Application data

Typed request → application protobuf → Syntra RPC → encrypted TCP → response
correlation → application decode. Notifications are opaque reconciliation hints
or validated ephemeral connectivity observations. Per-target `GetTargetChanges`
pages hydrate with `GetMessages`, then cache rows and accept positions commit
atomically. Paged history repair retains continuation, window budgets and scope
fences. History and dialog snapshots never forge journal progress.

## Platforms and fixtures

UniFFI wraps the native client over one process-wide Tokio runtime. Generated
Swift/Kotlin configuration uses one-use trust objects for secret inputs.
`libtinkle-web` exports portable schema checks and explicit unsupported network
status, with no native client dependency.

Rust tests and the host-only mobile fixture use published `syntra-net::tcp`
accept/connect APIs. No fixture implements channel cryptography. The mobile
fixture's separate loopback control socket is a host test harness, never an
application transport or a production SDK feature.
