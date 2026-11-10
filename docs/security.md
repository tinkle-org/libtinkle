# Native transport and trust

Application traffic uses encrypted Syntra TCP. In `libtinkle::ClientConfig`, `primary_port`
defaults to 443, and optional `fallback_port` defaults to `Some(80)` (`None`
disables fallback). Fallback is available only after an initial primary dial
failure, before any socket has been selected. Custom ports use the same encryption
and trust requirements: one provisioned server public key, server key ID, exact
UTF-8 scope, and explicit channel mode. Failure after TCP selection but before
authenticated readiness parks automatic reconnect, including EOF and timeout:
remote first-flight rejection can look identical to network loss. Typed
trust/protocol failures remain terminal for automatic reconnect after readiness.
There is
no HTTP/TLS wrapper, alternate carrier, insecure mode or mode downgrade.

After socket selection the chosen port remains fixed for reconnects. The
TCP-only SDK does not publicly expose its private pump failure classification,
so a post-ready close cannot authorize alternate-port selection either.
Successful explicit trust replacement within the same deployment resets this
local route choice; its scope restrictions are described below.

`ServerTrust` validates a nonzero 32-byte pin and 1..=255-byte scope before I/O.
`TrustSource::Bootstrap` has no AK; `TrustSource::Returning` requires ACTIVE status and a nonzero
32-byte authorization key, nonzero 8-byte handle and positive epoch. The
published SDK owns all cryptographic operations and checks. Rejected Returning
proof must be resolved by explicit host policy, never by switching to Bootstrap.

`crates/libtinkle/src/retirement.rs` observes negative-only retirement at
conditional restore/foreground entry and after coalesced loss or denial of the
current foreground Returning generation. A 60-second offline fallback skips
healthy Returning sockets. Before status Bootstrap opens, the old transport is
retired and joined; the query uses the same provisioned trust and selected route.
Failure is only a reason to seek evidence, never evidence itself. Endpoint proof
rejection parks all uses; unverified Bootstrap establishment also parks
conservatively. UNKNOWN and errors retain credentials. Exact current negative
evidence is rechecked against source and protected-store revisions before
conditional local cleanup; it never confirms a prior remote Logout.

Capability-bearing provisioning children share the parent's rejection fence and
proof serialization. A parked parent cannot dial a fresh child or send a retained
capability; child rejection also parks parent/sibling uses. This is independent of
foreground scheduling and clears only through explicit configuration recovery.

`crates/libtinkle/src/client.rs` also preserves an active provisioning caller's
registered slot against ordinary admission and retirement observation. Abandoned
or explicitly closing children are joined before replacement. An incomplete
carrier/policy shutdown retains its owner and blocks a new dial; neither timeout
nor a bounded lifecycle return proves join completion.

Conditional credential adoption requires host install exclusion: an atomic
incarnation/revision/full-record comparison plus a permit held through adoption
and Returning setup. Namespace mutation must wait or reject until release.
Cancellation fences the old runtime before releasing its permit. This provides
ordered installation, not continuous cross-process revocation of an already
installed session after a later account switch; that switch also requires host
coordination of the previous Client. Separate rereads do not provide this guarantee.

## Secret ownership

`Enrollment` is non-Clone, zeroized on drop and redacted in Debug. The client
shares its source behind a lock and creates a fresh SDK `Secret32` per attempt.
Core hello credentials are empty; authorization comes from the channel mode and
proof. Neither keys nor enrollment tuples are written into application SQLite,
journal/cursor stores, URLs, headers or logs.

The host stores enrollment material separately in protected Keychain/Keystore
storage, bound to scope and server trust, and supplies it on process restart.

`libtinkle::Client::replace_trust_source` accepts only an identical `ServerTrust`
(server pin, key ID, and exact scope); a different deployment requires a new client
with a separate cache/cursor namespace. Raw replacement retires the old connection,
clears the application principal and retained targets, watches, and pending
catch-up state, and installs the supplied source. It does not establish a verified
user identity; normal activation/restoration uses the typed enrollment coordinator.

The UniFFI source is consumed once. Rust-side secret inputs are zeroized, but
foreign-language input copies are host-owned and must also be erased.

The typed enrollment coordinator persists operation/capability before SignIn,
waits for atomic secure candidate storage, and activates on pinned Bootstrap
using that retained capability. It validates the exact ACTIVE acknowledgement,
retires old transport material, persists ACTIVE atomically, and establishes fresh
Returning. Bootstrap alone is not an authenticated user session. See
`device-enrollment.md` for platform acknowledgement and process-sharing contracts.

## Errors and application boundaries

Local network errors are sanitized to retryable unavailability. Trust/protocol
rejection is terminal for automatic reconnect. Application RPC errors preserve
typed codes; hosts never classify failures by diagnostic strings. Transport
receipt does not prove application persistence. In-flight calls lost with a
connection retain unknown-outcome semantics; unsafe requests require explicit
application idempotency policy before retry.

End-to-end application encryption is independent of the native channel.
Application ciphertext, when present, crosses the SDK as opaque payload bytes.
