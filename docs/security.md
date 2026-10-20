# Security

## Transport

- Production requires `wss://` (TLS 1.3 via rustls, native roots, full
  chain + hostname + expiry verification, fail closed). `ws://` is
  restricted-development only (loopback/isolated nets, explicit
  `allow_insecure`, warning logged, never public traffic).
- No `wss://` → `ws://` downgrade retry (FORBIDDEN). Version/capability
  downgrades that weaken transport security are refused.
- No Syntra byte before TLS + 101. SNI is sent consistently; proxy
  termination legs must be explicitly secured and documented.

## Credentials

- Opaque secret bytes travel only as `ClientHello.credential`. Never in
  URLs, query, headers, methods, errors, or logs. `Endpoint::parse`
  rejects credential-pattern query/userinfo.
- `Credential` redacts `Debug` (`Credential([redacted])`), zeroizes on
  drop (`zeroize`), and is cloned only for the handshake write.
- Generated prost `Debug` output can leak credential bytes; this SDK
  never logs `ClientHello` debug strings, raw `Frame` bytes, or
  `RpcError.message` verbatim in untrusted contexts.
- Panic reports, event callbacks, and diagnostics carry generic codes,
  never secrets. `connection_id` is an opaque diagnostic (≥128-bit
  entropy server-side), never an authenticator or resume token.
- Persistence: this crate never persists credentials. Apps store session
  tokens in Keychain/Keystore/EncryptedSharedPreferences (never
  cleartext prefs, never `localStorage` secrets, never URL params).

## Application crypto boundary

No Signal cryptography is implemented as part of Syntra. If Tinkle E2EE
is integrated later, ciphertext is constructed at the application
encryption layer (established libraries, audited ratchets) before
entering Syntra; Syntra carries it opaquely, never parses or decrypts
it. Transport receipt never proves application processing; only
`RpcResult{payload}` proves success.

## Error hygiene

`RpcError.message` is public generic text (branch on `code`, not text).
No stack traces, paths, SQL, tokens, or raw bytes in peer-visible
errors. Unknown future codes map to `UNKNOWN` with the numeric preserved
diagnostically (never success).
