//! Owning-domain durable action adapters; unsupported schema capabilities fail before transmission.
use crate::transfers::TransferControl;
use crate::{FfiOutgoingJob, FfiSendError, TinkleClient};
#[cfg(tinkle_async_actions)]
use prost::Message;
use std::sync::Arc;

macro_rules! enqueue {
    ($name:ident, $request:ty, $method:literal, $enqueue:ident) => {
        #[uniffi::export(async_runtime = "tokio")]
        impl TinkleClient {
            /// Persist this domain's exact original request and keys before any transmission.
            pub async fn $name(
                &self,
                job_id: String,
                request: Vec<u8>,
            ) -> Result<FfiOutgoingJob, FfiSendError> {
                #[cfg(tinkle_async_actions)]
                {
                    tinkle_schema::wire::validate_request($method, &request)
                        .map_err(|_| FfiSendError::InvalidRequest)?;
                    // Raw persistence retains legal field ordering and explicit scalar presence.
                    self.core_client()
                        .sending()
                        .enqueue_action_raw(job_id, $method.into(), request)
                        .await
                        .map(Into::into)
                        .map_err(Into::into)
                }
                #[cfg(not(tinkle_async_actions))]
                {
                    let _ = (job_id, request);
                    Err(FfiSendError::Media {
                        error: crate::FfiMediaError::Unsupported,
                    })
                }
            }
        }
    };
}
enqueue!(
    sending_enqueue_chat_photo,
    p::EditChatPhotoRequest,
    "/tinkle.v1.MessagesService/EditChatPhoto",
    enqueue_chat_photo
);
enqueue!(
    sending_enqueue_channel_photo,
    p::EditPhotoRequest,
    "/tinkle.v1.ChannelsService/EditPhoto",
    enqueue_channel_photo
);
enqueue!(
    sending_enqueue_profile_photo,
    p::SetProfilePhotoRequest,
    "/tinkle.v1.AccountService/SetProfilePhoto",
    enqueue_profile_photo
);
enqueue!(
    sending_enqueue_create_asset,
    p::CreateStickerAssetRequest,
    "/tinkle.v1.StickersService/CreateStickerAsset",
    enqueue_create_asset
);
enqueue!(
    sending_enqueue_update_asset,
    p::UpdateStickerAssetRequest,
    "/tinkle.v1.StickersService/UpdateStickerAsset",
    enqueue_update_asset
);

macro_rules! recover {
    ($name:ident, $method:ident) => {
        #[uniffi::export(async_runtime = "tokio")]
        impl TinkleClient {
            /// Query or cancel this domain's retained keys. A canceled waiter is not domain cancellation.
            pub async fn $name(
                &self,
                job_id: String,
                control: Arc<TransferControl>,
            ) -> Result<FfiOutgoingJob, FfiSendError> {
                #[cfg(tinkle_async_actions)]
                {
                    let mut lease = control
                        .begin(crate::transfers::FfiTransferPhase::Sending)
                        .map_err(|error| FfiSendError::Rpc { error })?;
                    let result = self
                        .core_client()
                        .sending()
                        .$method(job_id, Some(control.token()))
                        .await
                        .map(Into::into)
                        .map_err(Into::into);
                    lease.finish(result.is_ok(), None);
                    result
                }
                #[cfg(not(tinkle_async_actions))]
                {
                    let _ = (job_id, control);
                    Err(FfiSendError::Media {
                        error: crate::FfiMediaError::Unsupported,
                    })
                }
            }
        }
    };
}
recover!(sending_get_send_status, get_send_status);
recover!(sending_cancel_send, cancel_send);
recover!(sending_get_chat_photo_update, get_chat_photo_update);
recover!(sending_cancel_chat_photo_update, cancel_chat_photo_update);
recover!(sending_get_channel_photo_update, get_channel_photo_update);
recover!(
    sending_cancel_channel_photo_update,
    cancel_channel_photo_update
);
recover!(sending_get_profile_photo_update, get_profile_photo_update);
recover!(
    sending_cancel_profile_photo_update,
    cancel_profile_photo_update
);
recover!(sending_get_asset_change, get_asset_change);
recover!(sending_cancel_asset_change, cancel_asset_change);

macro_rules! inspect {
    ($name:ident, $method:ident) => {
        #[uniffi::export(async_runtime = "tokio")]
        impl TinkleClient {
            /// Offline retained status protobuf, never a current body/file authorization decision.
            pub async fn $name(&self, job_id: String) -> Result<Option<Vec<u8>>, FfiSendError> {
                #[cfg(tinkle_async_actions)]
                {
                    self.core_client()
                        .sending()
                        .$method(job_id)
                        .await
                        .map(|s| s.map(|v| v.encode_to_vec()))
                        .map_err(Into::into)
                }
                #[cfg(not(tinkle_async_actions))]
                {
                    let _ = job_id;
                    Err(FfiSendError::Media {
                        error: crate::FfiMediaError::Unsupported,
                    })
                }
            }
        }
    };
}
inspect!(sending_retained_send_status, retained_send_status);
inspect!(
    sending_retained_chat_photo_update,
    retained_chat_photo_update
);
inspect!(
    sending_retained_channel_photo_update,
    retained_channel_photo_update
);
inspect!(
    sending_retained_profile_photo_update,
    retained_profile_photo_update
);
inspect!(sending_retained_asset_change, retained_asset_change);

macro_rules! direct {
    ($name:ident,$owner:ident,$method:ident,$request:ident,$service:literal,$wire:literal) => {
        #[uniffi::export(async_runtime = "tokio")]
        impl TinkleClient {
            /// Actor-private query/cancel with the schema's original domain selector, not a local job ID.
            pub async fn $name(
                &self,
                request: Vec<u8>,
                control: Arc<TransferControl>,
            ) -> Result<Vec<u8>, FfiSendError> {
                #[cfg(tinkle_async_actions)]
                {
                    if request.len() > 16384 {
                        return Err(FfiSendError::InvalidRequest);
                    }
                    tinkle_schema::wire::validate_request(
                        concat!("/tinkle.v1.", $service, "/", $wire),
                        &request,
                    )
                    .map_err(|_| FfiSendError::InvalidRequest)?;
                    let request = tinkle_schema::proto::$request::decode(request.as_slice())
                        .map_err(|_| FfiSendError::InvalidRequest)?;
                    let mut lease = control
                        .begin(crate::transfers::FfiTransferPhase::Sending)
                        .map_err(|error| FfiSendError::Rpc { error })?;
                    let result = self
                        .core_client()
                        .$owner()
                        .$method(request, Some(control.token()))
                        .await
                        .map(|r| r.encode_to_vec())
                        .map_err(|e| FfiSendError::from(libtinkle::SendError::from(e)));
                    lease.finish(result.is_ok(), None);
                    result
                }
                #[cfg(not(tinkle_async_actions))]
                {
                    let _ = (request, control);
                    Err(FfiSendError::Media {
                        error: crate::FfiMediaError::Unsupported,
                    })
                }
            }
        }
    };
}
direct!(
    messages_get_send_status,
    messages,
    get_send_status,
    GetSendStatusRequest,
    "MessagesService",
    "GetSendStatus"
);
direct!(
    messages_cancel_send,
    messages,
    cancel_send,
    CancelSendRequest,
    "MessagesService",
    "CancelSend"
);
direct!(
    messages_get_chat_photo_update,
    messages,
    get_chat_photo_update,
    GetChatPhotoUpdateRequest,
    "MessagesService",
    "GetChatPhotoUpdate"
);
direct!(
    messages_cancel_chat_photo_update,
    messages,
    cancel_chat_photo_update,
    CancelChatPhotoUpdateRequest,
    "MessagesService",
    "CancelChatPhotoUpdate"
);
direct!(
    channels_get_photo_update,
    channels,
    get_photo_update,
    GetPhotoUpdateRequest,
    "ChannelsService",
    "GetPhotoUpdate"
);
direct!(
    channels_cancel_photo_update,
    channels,
    cancel_photo_update,
    CancelPhotoUpdateRequest,
    "ChannelsService",
    "CancelPhotoUpdate"
);
direct!(
    account_get_profile_photo_update,
    account,
    get_profile_photo_update,
    GetProfilePhotoUpdateRequest,
    "AccountService",
    "GetProfilePhotoUpdate"
);
direct!(
    account_cancel_profile_photo_update,
    account,
    cancel_profile_photo_update,
    CancelProfilePhotoUpdateRequest,
    "AccountService",
    "CancelProfilePhotoUpdate"
);
direct!(
    stickers_get_asset_change,
    stickers,
    get_asset_change,
    GetAssetChangeRequest,
    "StickersService",
    "GetAssetChange"
);
direct!(
    stickers_cancel_asset_change,
    stickers,
    cancel_asset_change,
    CancelAssetChangeRequest,
    "StickersService",
    "CancelAssetChange"
);

#[uniffi::export(async_runtime = "tokio")]
impl TinkleClient {
    /// Fetch an exact current owner/version projection; never hydrate from an obsolete action body.
    pub async fn media_refresh_representation(
        &self,
        reference: Vec<u8>,
        main_file_id: i64,
    ) -> Result<Vec<u8>, FfiSendError> {
        #[cfg(tinkle_async_actions)]
        {
            if reference.len() > 16384 {
                return Err(FfiSendError::InvalidRequest);
            }
            let reference = tinkle_schema::proto::FileAccessContext::decode(reference.as_slice())
                .map_err(|_| FfiSendError::InvalidRequest)?;
            self.core_client()
                .refresh_media_representation(reference, main_file_id)
                .await
                .map(|r| r.encode_to_vec())
                .map_err(Into::into)
        }
        #[cfg(not(tinkle_async_actions))]
        {
            let _ = (reference, main_file_id);
            Err(FfiSendError::Media {
                error: crate::FfiMediaError::Unsupported,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client() -> Arc<TinkleClient> {
        TinkleClient::new_sqlite(
            crate::ClientConfigFfi {
                host: "127.0.0.1".into(),
                primary_port: 1,
                fallback_port: None,
                client_info: crate::ClientInfoFfi {
                    device_model: "test".into(),
                    platform: "test".into(),
                    os_version: "1".into(),
                    app_version: "1".into(),
                    app_build: "1".into(),
                    options: crate::ClientOptionsFfi {
                        agent: String::new(),
                        device_id: String::new(),
                        install_id: String::new(),
                        accept_language: String::new(),
                        time_zone: String::new(),
                        user_agent: String::new(),
                    },
                },
            },
            crate::TrustSourceFfi::bootstrap(crate::ServerTrustFfi {
                server_public_key: vec![7; 32],
                server_key_id: 1,
                scope: "action-fixture".into(),
            })
            .unwrap(),
            crate::sqlite::SqliteStorage::open_in_memory().unwrap(),
        )
        .unwrap()
    }

    /// New action symbols are callable but explicitly unsupported against the unchanged published pin.
    #[cfg(not(tinkle_async_actions))]
    #[tokio::test]
    async fn published_pin_never_downgrades_a_new_action() {
        let client = client();
        assert!(matches!(
            client
                .sending_enqueue_profile_photo("profile".into(), vec![])
                .await,
            Err(FfiSendError::Media {
                error: crate::FfiMediaError::Unsupported
            })
        ));
        assert!(matches!(
            client
                .sending_get_send_status("send".into(), TransferControl::new())
                .await,
            Err(FfiSendError::Media {
                error: crate::FfiMediaError::Unsupported
            })
        ));
        client.core_client().close().await;
    }

    /// The candidate adapter persists exact UUID/CAS bytes; unknown fields do not get stripped.
    #[cfg(tinkle_async_actions)]
    #[tokio::test]
    async fn profile_adapter_preserves_request_and_rejects_unknown_fields() {
        use tinkle_schema::proto as p;
        let client = client();
        client.set_principal_user_id(42).await.unwrap();
        let request = p::SetProfilePhotoRequest {
            request_id: "11111111-1111-4111-8111-111111111111".into(),
            expected_revision: 7,
            source: Some(p::ProfilePhotoSource {
                file_id: 9,
                metadata: Some(p::ImageMetadata {
                    width: 2,
                    height: 2,
                    format: 1,
                    validation: Some(p::MediaValidation {
                        profile: 1,
                        version: 1,
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }),
        };
        let mut bytes = request.encode_to_vec();
        bytes.extend([0x10, 7]);
        let job = client
            .sending_enqueue_profile_photo("profile".into(), bytes.clone())
            .await
            .unwrap();
        assert_eq!(job.request, bytes);
        assert!(matches!(job.state, crate::FfiOutgoingState::ActionPrepared));
        bytes.extend([0x98, 0x06, 1]);
        assert!(matches!(
            client
                .sending_enqueue_profile_photo("bad".into(), bytes)
                .await,
            Err(FfiSendError::InvalidRequest)
        ));
        assert_eq!(client.sending_jobs().await.unwrap().len(), 1);
        client.core_client().close().await;
    }

    /// Fixed nonzero signed send keys are opaque identities, not positive IDs or regenerated values.
    #[cfg(tinkle_async_actions)]
    #[tokio::test]
    async fn frontend_send_adapters_preserve_signed_extremes_and_typed_domain_selectors() {
        use tinkle_schema::proto as p;
        let client = client();
        client.set_principal_user_id(42).await.unwrap();
        let target = Some(p::Target {
            kind: Some(p::target::Kind::ChatId(7)),
        });
        let media = Some(p::MediaSource {
            media: Some(p::media_source::Media::Photo(p::PhotoSource {
                file_id: 9,
                ..Default::default()
            })),
        });
        for key in [i64::MIN, -1, i64::MAX] {
            let original = p::SendMediaRequest {
                target,
                media: media.clone(),
                idempotency_key: key,
                ..Default::default()
            }
            .encode_to_vec();
            let job = client
                .sending_enqueue_media(format!("fixed-{key}"), original.clone())
                .await
                .unwrap();
            let repeated = client
                .sending_enqueue_media(format!("fixed-{key}"), original.clone())
                .await
                .unwrap();
            assert_eq!(job.request, original);
            assert_eq!(repeated.request, job.request);
            assert_eq!(
                p::SendMediaRequest::decode(job.request.as_slice())
                    .unwrap()
                    .idempotency_key,
                key
            );
            // Valid selectors reach connection admission; zero is rejected by domain validation.
            assert!(matches!(
                client
                    .messages_get_send_status(
                        p::GetSendStatusRequest {
                            target,
                            idempotency_key: key
                        }
                        .encode_to_vec(),
                        TransferControl::new()
                    )
                    .await,
                Err(FfiSendError::Rpc {
                    error: crate::FfiError::NotConnected
                })
            ));
            assert!(matches!(
                client
                    .messages_cancel_send(
                        p::CancelSendRequest {
                            target,
                            idempotency_key: key
                        }
                        .encode_to_vec(),
                        TransferControl::new()
                    )
                    .await,
                Err(FfiSendError::Rpc {
                    error: crate::FfiError::NotConnected
                })
            ));
        }
        assert!(matches!(
            client
                .messages_get_send_status(
                    p::GetSendStatusRequest {
                        target,
                        idempotency_key: 0
                    }
                    .encode_to_vec(),
                    TransferControl::new()
                )
                .await,
            Err(FfiSendError::Rpc {
                error: crate::FfiError::Codec(_)
            })
        ));
        let album = p::SendAlbumRequest {
            target,
            items: [i64::MIN, i64::MAX]
                .into_iter()
                .map(|idempotency_key| p::AlbumItem {
                    idempotency_key,
                    media: media.clone(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
        .encode_to_vec();
        let retained = client
            .sending_enqueue_album("album-extremes".into(), album.clone())
            .await
            .unwrap();
        assert_eq!(retained.request, album);
        assert_eq!(
            p::SendAlbumRequest::decode(retained.request.as_slice())
                .unwrap()
                .items
                .iter()
                .map(|i| i.idempotency_key)
                .collect::<Vec<_>>(),
            [i64::MIN, i64::MAX]
        );
        let request_id = "11111111-1111-4111-8111-111111111111".to_owned();
        macro_rules! reaches_admission {
            ($method:ident,$request:expr) => {
                assert!(matches!(
                    client
                        .$method($request.encode_to_vec(), TransferControl::new())
                        .await,
                    Err(FfiSendError::Rpc {
                        error: crate::FfiError::NotConnected
                    })
                ));
            };
        }
        reaches_admission!(
            messages_get_chat_photo_update,
            p::GetChatPhotoUpdateRequest {
                chat_id: 7,
                operation_id: 19
            }
        );
        reaches_admission!(
            messages_cancel_chat_photo_update,
            p::CancelChatPhotoUpdateRequest {
                chat_id: 7,
                operation_id: 19
            }
        );
        reaches_admission!(
            channels_get_photo_update,
            p::GetPhotoUpdateRequest {
                channel_id: 7,
                request_id: request_id.clone()
            }
        );
        reaches_admission!(
            channels_cancel_photo_update,
            p::CancelPhotoUpdateRequest {
                channel_id: 7,
                request_id: request_id.clone()
            }
        );
        reaches_admission!(
            account_get_profile_photo_update,
            p::GetProfilePhotoUpdateRequest {
                request_id: request_id.clone()
            }
        );
        reaches_admission!(
            account_cancel_profile_photo_update,
            p::CancelProfilePhotoUpdateRequest {
                request_id: request_id.clone()
            }
        );
        reaches_admission!(
            stickers_get_asset_change,
            p::GetAssetChangeRequest {
                request_id: request_id.clone(),
                asset_id: Some(31)
            }
        );
        reaches_admission!(
            stickers_cancel_asset_change,
            p::CancelAssetChangeRequest {
                request_id,
                asset_id: Some(31)
            }
        );
        client.core_client().close().await;
    }
}
