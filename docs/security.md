# Native transport and trust

Application traffic uses encrypted Syntra TCP on 443, with 80 available only
after a primary dial failure. Both ports use one provisioned server public key,
server key ID, exact UTF-8 scope and explicit channel mode. Once a socket
connects, channel/authentication/protocol failures park automatic reconnect.
EOF and timeout before authenticated readiness also park: remote first-flight
rejection can look identical to network loss. There is
no HTTP/TLS wrapper, alternate carrier, insecure mode or mode downgrade.

After socket selection the chosen port remains fixed for reconnects. The
TCP-only SDK does not publicly expose its private pump failure classification,
so a post-ready close cannot authorize alternate-port selection either.
Explicit trust replacement resets this local route choice.

`ServerTrust` validates a nonzero 32-byte pin and 1..=255-byte scope before I/O.
`TrustSource::Bootstrap` has no AK; `TrustSource::Returning` requires ACTIVE status and a nonzero
32-byte authorization key, nonzero 8-byte handle and positive epoch. The
published SDK owns all cryptographic operations and checks. Rejected Returning
proof must be resolved by explicit host policy, never by switching to Bootstrap.

Permanent-retirement observation has its own pre-authorized foreground schedule:
status-first restore/foreground entry and a fixed 60-second interval, established
before any Returning outcome. It opens only a bounded status-only Bootstrap
connection using the same provisioned trust and selected route. Returning failure
cannot schedule, accelerate or reset it. Endpoint proof rejection parks both
users; unverified Bootstrap establishment also parks conservatively. Neither
UNKNOWN nor an error erases credentials. Exact current negative evidence is
rechecked against local source and durable namespace revisions before retained,
conditional local cleanup. It never confirms remote Logout.

Capability-bearing provisioning children share the parent's rejection fence and
proof serialization. A parked parent cannot dial a fresh child or send a retained
capability; child rejection also parks parent/sibling uses. This is independent of
foreground scheduling and clears only through explicit configuration recovery.

Conditional credential adoption requires host install exclusion: an atomic
incarnation/revision/full-record comparison plus a permit held through adoption
and Returning setup. Namespace mutation must wait or reject until release.
Cancellation fences the old runtime before releasing its permit. This provides
ordered installation, not continuous cross-process revocation of an already
installed session after a later account switch; that switch also requires host
coordination of the previous Client. Legacy rereads do not provide this guarantee.

## Secret ownership

`Enrollment` is non-Clone, zeroized on drop and redacted in Debug. The client
shares its source behind a lock and creates a fresh SDK `Secret32` per attempt.
Core hello credentials are empty; authorization comes from the channel mode and
proof. Neither keys nor enrollment tuples are written into application SQLite,
journal/cursor stores, URLs, headers or logs.

The host stores enrollment material separately in protected Keychain/Keystore
storage, bound to scope and server trust, and supplies it on process restart.
`replace_trust_source` retires the old connection before installing new material.
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
