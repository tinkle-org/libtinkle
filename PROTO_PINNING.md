# Proto pinning (BSR Rust SDKs via public Cargo index)

This workspace consumes BSR-generated bindings directly, with no
vendored `.proto` copies and no local `prost-build` codegen. The public
index needs no token. Authenticated-index credentials are unnecessary
for these public modules.

Registry (`.cargo/config.toml`):

```toml
[registries.buf]
index = "sparse+https://buf.build/gen/cargo/public/"
```

## Syntra (published crates, no BSR binding)

- Syntra protocol types now come from published `syntra-core`
  `=1.0.0-rc.1` (with `syntra-runtime` / `syntra-ws` at the same pin).
- Syntra proto runtime: `prost` `0.13.5` internally. Tinkle uses BSR prost `0.14`.
- Boundary is opaque `Vec<u8>`: Tinkle payload bytes cross into Syntra
  frames without sharing prost types.

## Tinkle (`buf.build/tinkle-org/schema`)

- Module commit: `782100705bf94b889fb9b5fcea7efa44` (adds `FilesService`
  upload/download + typed media `*Source` attachments)
- Prost data crate: `tinkle-org_schema_community_neoeinstein-prost`
  pinned `0.5.0-20261026025606-782100705bf9.2`
  (`buf registry sdk version --module=buf.build/tinkle-org/schema --plugin=buf.build/community/neoeinstein-prost`)
- Tonic client crate: `tinkle-org_schema_community_neoeinstein-tonic`
  pinned `0.5.0-20261026025606-782100705bf9.4`
  (`buf registry sdk version --module=buf.build/tinkle-org/schema --plugin=buf.build/community/neoeinstein-tonic`)
- Runtime: `prost` `0.14`, `tonic` `0.14`.
- Method identifiers (`tinkle-schema::methods`, 179 entries = 172 prior +
  7 `FilesService`) verified against these service definitions. Failures
  use Syntra `RpcResult.error`.
- `UpdatesService` is connectivity-only: `ObserveConnectivity`
  (server-streaming; delivered over Syntra notifications and decoded by
  `tinkle-schema::sync::validate_connectivity_notification`). Durable
  replay/watch (`ReadUpdates`, `FollowUpdates`, `Update` envelope,
  `RebuildNeeded`) are retired from the public contract and absent here.
- Reconciliation (`78e0f25091b8`): per-conversation, not per-stream.
  `Message.accept_seq` is the per-conversation ordering/dedup token
  (absent ⇒ order by date, never a dedup vote);
  `GetTargetChanges(after_accept_seq, limit 1..500)` pages the retained
  edit/delete/revocation journal (`more_available` tri-state, `last_accept_seq`
  cursor); `GetMessages` fetches ≤100 bodies per conversation (fresher
  `edit_date` wins; absent never beats dated);
  `GetHistoryResponse.more_available` confirms history continuation.
  `tinkle-schema::sync` validates journal pages (`validate_changes_page`),
  classifies accept positions (`accept_seq_decision`), chunks fetches
  (`chunk_message_ids`), and keys conversations (`target_key_for`:
  `target:user:<id>` / `target:room:<id>`).
- Naming (`#52`): `Target` (`target::Kind::{UserId, ChatId}`) replaces
  `PeerInput`; request fields are `target` (`from_target` on search).
  Numeric `*_peer_id` fields (`offset_peer_id`, `reply_to_peer_id`) are
  unchanged by the schema and stay as-is.
- Files (`782100705bf9`): new `FilesService` (7 unary RPCs:
  `BeginUpload` / `UploadPart` / `GetUploadStatus` / `CompleteUpload` /
  `AbortUpload` / `GetFileInfo` / `DownloadPart`). `BeginUploadResponse`
  carries `status` (`UPLOAD_STATUS_*`) with presence rules:
  `part_size_bytes` / `max_parallel_parts` / `expires_at` present iff
  `ACTIVE`; `file` (`FileInfo`) present iff `COMPLETED` (including
  zero-byte uploads). `GetFileInfoResponse` carries `file` plus
  `max_download_part_bytes` (`Option<i32>`, matches
  `DownloadPartRequest.limit`; absent = non-conforming, fall back to a
  conservative local limit, reject only nonpositive limits).
- Media attachments: legacy `Input*` / `MediaInput` envelopes retired;
  replaced by `MediaSource` oneof (`Photo` / `Document` / `Geo` /
  `Contact` / `Poll` / `Video` / `Audio` / `Voice` / `Sticker`) plus
  typed `PhotoSource` / `DocumentSource` / `VideoSource` / `AudioSource` /
  `VoiceSource` / `StickerSource` / `ContactSource` / `GeoPointSource` /
  `PollSource`, `SingleMediaSource`, `MediaAttribute`
  (`ImageMetadata` / `VideoMetadata` / `AudioMetadata` /
  `StickerMetadata`), and `MediaVariant` / `MediaVariantSource`.
  Bot `SendPhoto` / `SendDocument` / `SendVideo` / `SendAudio` /
  `SendVoice` / `SendSticker` keep the legacy scalar file ID alongside the
  typed `*_source` in a `*_form` oneof; `EditMessageMediaRequest.media_form`
  is legacy `BotMediaSource` vs typed `MediaSource`.
- Dialogs: `GetDialogsRequest` supports legacy
  (`offset_date` + `offset_peer_id`) or opaque `cursor` pagination (mutually
  exclusive; keep `space_id` scope stable across pages);
  `GetDialogsResponse.next_cursor` nonempty means pagination may continue
  (do not infer exhaustion from page size). `SetHistoryTTL` RPC keeps its
  path; prost types are `SetHistoryTtlRequest` / `SetHistoryTtlResponse`
  (`Ttl` casing).

## Upgrading

Syntra upgrades: first update the exact version requirements for
`syntra-core`, `syntra-runtime`, and `syntra-ws` in `Cargo.toml`,
then run `cargo update` and `cargo test`.

Tinkle binding only:

```sh
buf registry sdk version --module=buf.build/tinkle-org/schema --plugin=buf.build/community/neoeinstein-prost
buf registry sdk version --module=buf.build/tinkle-org/schema --plugin=buf.build/community/neoeinstein-tonic
```

Copy the versions verbatim into the workspace `Cargo.toml`. Do not
construct them by hand. `cargo update -p <crate>` then `cargo test --workspace`.
