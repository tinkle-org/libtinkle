//! Nameful protobuf adapters: decode the exact request, validate in the typed core,
//! and return the corresponding response bytes. No arbitrary method dispatch here.
use crate::{FfiError, TinkleClient};
use prost::Message as _;
use tinkle_schema::proto as p;

/// Export concrete request adapters, preserving the application's protobuf boundary.
macro_rules! service_methods {
    ($facade:ident; $( $name:ident => $method:ident($request:ident); )*) => {
        #[uniffi::export(async_runtime = "tokio")]
        impl TinkleClient { $(
            /// Decode the named request, validate locally, and invoke once. Returns its response protobuf.
            pub async fn $name(&self, request: Vec<u8>) -> Result<Vec<u8>, FfiError> {
                if request.len() > 262144 { return Err(FfiError::Codec("request exceeds service budget".into())); }
                let request = p::$request::decode(request.as_slice()).map_err(|_| FfiError::Codec(concat!("invalid ",stringify!($request)).into()))?;
                Ok(self.core_client().$facade().$method(request).await.map_err(FfiError::from)?.encode_to_vec())
            }
        )* }
    };
}

service_methods! { messages;
    messages_get_scheduled_messages => get_scheduled_messages(GetScheduledMessagesRequest);
    messages_cancel_scheduled_messages => cancel_scheduled_messages(CancelScheduledMessagesRequest);
    messages_close_poll => close_poll(ClosePollRequest);
    messages_get_recent_reactions => get_recent_reactions(GetRecentReactionsRequest);
    messages_get_dialogs => get_dialogs(GetDialogsRequest);
    messages_get_history => get_history(GetHistoryRequest);
    messages_get_messages => get_messages(GetMessagesRequest);
    messages_list_message_changes => list_message_changes(ListMessageChangesRequest);
    messages_mark_history_read => mark_history_read(MarkHistoryReadRequest);
    messages_delete_history => delete_history(DeleteHistoryRequest);
    messages_delete_messages => delete_messages(DeleteMessagesRequest);
    messages_create_chat => create_chat(CreateChatRequest);
    messages_edit_chat_title => edit_chat_title(EditChatTitleRequest);
    messages_edit_chat_photo => edit_chat_photo(EditChatPhotoRequest);
    messages_update_chat_description => update_chat_description(UpdateChatDescriptionRequest);
    messages_get_chats => get_chats(GetChatsRequest);
    messages_get_chat_details => get_chat_details(GetChatDetailsRequest);
    messages_add_chat_participant => add_chat_participant(AddChatParticipantRequest);
    messages_remove_chat_participant => remove_chat_participant(RemoveChatParticipantRequest);
    messages_edit_chat_admin => edit_chat_admin(EditChatAdminRequest);
    messages_edit_chat_restricted_scopes => edit_chat_restricted_scopes(EditChatRestrictedScopesRequest);
    messages_set_history_ttl => set_history_ttl(SetHistoryTtlRequest);
    messages_set_message_pin => set_message_pin(SetMessagePinRequest);
    messages_unpin_all_messages => unpin_all_messages(UnpinAllMessagesRequest);
    messages_send_message => send_message(SendMessageRequest);
    messages_send_media => send_media(SendMediaRequest);
    messages_send_album => send_album(SendAlbumRequest);
    messages_edit_message => edit_message(EditMessageRequest);
    messages_set_typing => set_typing(SetTypingRequest);
    messages_start_bot => start_bot(StartBotRequest);
    messages_search => search(SearchRequest);
    messages_batch_get_message_reactions => batch_get_message_reactions(BatchGetMessageReactionsRequest);
    messages_list_message_reactions => list_message_reactions(ListMessageReactionsRequest);
    messages_send_reaction => send_reaction(SendReactionRequest);
    messages_get_unread_mentions => get_unread_mentions(GetUnreadMentionsRequest);
    messages_read_mentions => read_mentions(ReadMentionsRequest);
    messages_get_unread_reactions => get_unread_reactions(GetUnreadReactionsRequest);
    messages_read_reactions => read_reactions(ReadReactionsRequest);
    messages_send_vote => send_vote(SendVoteRequest);
    messages_get_poll_votes => get_poll_votes(GetPollVotesRequest);
    messages_get_poll_results => get_poll_results(GetPollResultsRequest);
    messages_read_thread => read_thread(ReadThreadRequest);
    messages_list_thread_messages => list_thread_messages(ListThreadMessagesRequest);
}

service_methods! { users;
    users_get_users => get_users(GetUsersRequest);
    users_get_user_complete => get_user_complete(GetUserCompleteRequest);
}

service_methods! { account;
    account_update_profile => update_profile(UpdateProfileRequest);
    account_update_username => update_username(UpdateUsernameRequest);
    account_validate_username => validate_username(ValidateUsernameRequest);
    account_set_profile_photo => set_profile_photo(SetProfilePhotoRequest);
    account_get_profile_photos => get_profile_photos(GetProfilePhotosRequest);
    account_select_profile_photo => select_profile_photo(SelectProfilePhotoRequest);
    account_delete_profile_photo => delete_profile_photo(DeleteProfilePhotoRequest);
}

service_methods! { stickers;
    stickers_get_installed_packs => get_installed_packs(GetInstalledPacksRequest);
    stickers_install_pack => install_pack(InstallPackRequest);
    stickers_uninstall_pack => uninstall_pack(UninstallPackRequest);
    stickers_reorder_packs => reorder_packs(ReorderPacksRequest);
    stickers_get_pack => get_pack(GetPackRequest);
    stickers_get_pack_by_short_name => get_pack_by_short_name(GetPackByShortNameRequest);
    stickers_search_packs => search_packs(SearchPacksRequest);
    stickers_get_archived_packs => get_archived_packs(GetArchivedPacksRequest);
    stickers_get_recent_stickers => get_recent_stickers(GetRecentStickersRequest);
    stickers_save_recent_sticker => save_recent_sticker(SaveRecentStickerRequest);
    stickers_clear_recent_stickers => clear_recent_stickers(ClearRecentStickersRequest);
    stickers_get_favorite_stickers => get_favorite_stickers(GetFavoriteStickersRequest);
    stickers_toggle_favorite_sticker => toggle_favorite_sticker(ToggleFavoriteStickerRequest);
    stickers_create_sticker_asset => create_sticker_asset(CreateStickerAssetRequest);
    stickers_update_sticker_asset => update_sticker_asset(UpdateStickerAssetRequest);
    stickers_delete_sticker_asset => delete_sticker_asset(DeleteStickerAssetRequest);
    stickers_get_sticker_assets => get_sticker_assets(GetStickerAssetsRequest);
    stickers_get_owned_packs => get_owned_packs(GetOwnedPacksRequest);
    stickers_create_pack => create_pack(CreatePackRequest);
    stickers_update_pack => update_pack(UpdatePackRequest);
    stickers_delete_pack => delete_pack(DeletePackRequest);
    stickers_add_pack_asset => add_pack_asset(AddPackAssetRequest);
    stickers_remove_pack_asset => remove_pack_asset(RemovePackAssetRequest);
    stickers_reorder_pack_assets => reorder_pack_assets(ReorderPackAssetsRequest);
    stickers_publish_pack => publish_pack(PublishPackRequest);
    stickers_unpublish_pack => unpublish_pack(UnpublishPackRequest);
}
