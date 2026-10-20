//! Canonical Syntra method identifiers for `tinkle.v1`.
//!
//! Each identifier is `/tinkle.v1.<Service>/<Method>` (SNTR-RPC-001).
//! Generated from the vendored `tinkle/v1/*.proto` service definitions;
//! do not hand-edit method strings. Payload types are the prost request/
//! response messages in `proto.rs`. Failures use Syntra `RpcResult.error`.

/// All known Tinkle methods with request/response type names.
/// `(method, request, response)`; descriptors for codegen.
pub const ALL_METHODS: &[(&str, &str, &str)] = &[
    (
        "/tinkle.v1.AccountService/RegisterPush",
        "RegisterPushRequest",
        "RegisterPushResponse",
    ),
    (
        "/tinkle.v1.AccountService/UnregisterPush",
        "UnregisterPushRequest",
        "UnregisterPushResponse",
    ),
    (
        "/tinkle.v1.AccountService/GetNotificationSettings",
        "GetNotificationSettingsRequest",
        "GetNotificationSettingsResponse",
    ),
    (
        "/tinkle.v1.AccountService/UpdateNotificationSetting",
        "UpdateNotificationSettingRequest",
        "UpdateNotificationSettingResponse",
    ),
    (
        "/tinkle.v1.AccountService/ResetNotificationSettings",
        "ResetNotificationSettingsRequest",
        "ResetNotificationSettingsResponse",
    ),
    (
        "/tinkle.v1.AccountService/GetPrivacy",
        "GetPrivacyRequest",
        "GetPrivacyResponse",
    ),
    (
        "/tinkle.v1.AccountService/SetPrivacy",
        "SetPrivacyRequest",
        "SetPrivacyResponse",
    ),
    (
        "/tinkle.v1.AccountService/UpdateProfile",
        "UpdateProfileRequest",
        "UpdateProfileResponse",
    ),
    (
        "/tinkle.v1.AccountService/ValidateUsername",
        "ValidateUsernameRequest",
        "ValidateUsernameResponse",
    ),
    (
        "/tinkle.v1.AccountService/UpdateUsername",
        "UpdateUsernameRequest",
        "UpdateUsernameResponse",
    ),
    (
        "/tinkle.v1.AccountService/DeleteAccount",
        "DeleteAccountRequest",
        "DeleteAccountResponse",
    ),
    (
        "/tinkle.v1.AccountService/SendChangePhoneCode",
        "SendChangePhoneCodeRequest",
        "SendChangePhoneCodeResponse",
    ),
    (
        "/tinkle.v1.AccountService/ChangePhoneNumber",
        "ChangePhoneNumberRequest",
        "ChangePhoneNumberResponse",
    ),
    (
        "/tinkle.v1.AccountService/GetContentSettings",
        "GetContentSettingsRequest",
        "GetContentSettingsResponse",
    ),
    (
        "/tinkle.v1.AccountService/SetContentSetting",
        "SetContentSettingRequest",
        "SetContentSettingResponse",
    ),
    (
        "/tinkle.v1.AccountService/Logout",
        "LogoutRequest",
        "LogoutResponse",
    ),
    (
        "/tinkle.v1.AccountService/RevokeSession",
        "RevokeSessionRequest",
        "RevokeSessionResponse",
    ),
    (
        "/tinkle.v1.AccountService/RevokeSessions",
        "RevokeSessionsRequest",
        "RevokeSessionsResponse",
    ),
    (
        "/tinkle.v1.AccountService/GetSessions",
        "GetSessionsRequest",
        "GetSessionsResponse",
    ),
    (
        "/tinkle.v1.AuthService/SendCode",
        "SendCodeRequest",
        "SendCodeResponse",
    ),
    (
        "/tinkle.v1.AuthService/SignIn",
        "SignInRequest",
        "SignInResponse",
    ),
    (
        "/tinkle.v1.BotApiService/GetMe",
        "GetMeRequest",
        "GetMeResponse",
    ),
    (
        "/tinkle.v1.BotApiService/SendMessage",
        "BotApiServiceSendMessageRequest",
        "BotApiServiceSendMessageResponse",
    ),
    (
        "/tinkle.v1.BotApiService/SendPhoto",
        "SendPhotoRequest",
        "SendPhotoResponse",
    ),
    (
        "/tinkle.v1.BotApiService/SendDocument",
        "SendDocumentRequest",
        "SendDocumentResponse",
    ),
    (
        "/tinkle.v1.BotApiService/SendVideo",
        "SendVideoRequest",
        "SendVideoResponse",
    ),
    (
        "/tinkle.v1.BotApiService/SendAudio",
        "SendAudioRequest",
        "SendAudioResponse",
    ),
    (
        "/tinkle.v1.BotApiService/SendVoice",
        "SendVoiceRequest",
        "SendVoiceResponse",
    ),
    (
        "/tinkle.v1.BotApiService/SendSticker",
        "SendStickerRequest",
        "SendStickerResponse",
    ),
    (
        "/tinkle.v1.BotApiService/SendLocation",
        "SendLocationRequest",
        "SendLocationResponse",
    ),
    (
        "/tinkle.v1.BotApiService/SendContact",
        "SendContactRequest",
        "SendContactResponse",
    ),
    (
        "/tinkle.v1.BotApiService/ForwardMessage",
        "ForwardMessageRequest",
        "ForwardMessageResponse",
    ),
    (
        "/tinkle.v1.BotApiService/CopyMessage",
        "CopyMessageRequest",
        "CopyMessageResponse",
    ),
    (
        "/tinkle.v1.BotApiService/EditMessageText",
        "EditMessageTextRequest",
        "EditMessageTextResponse",
    ),
    (
        "/tinkle.v1.BotApiService/EditMessageCaption",
        "EditMessageCaptionRequest",
        "EditMessageCaptionResponse",
    ),
    (
        "/tinkle.v1.BotApiService/EditMessageMedia",
        "EditMessageMediaRequest",
        "EditMessageMediaResponse",
    ),
    (
        "/tinkle.v1.BotApiService/EditMessageReplyMarkup",
        "EditMessageReplyMarkupRequest",
        "EditMessageReplyMarkupResponse",
    ),
    (
        "/tinkle.v1.BotApiService/DeleteMessage",
        "DeleteMessageRequest",
        "DeleteMessageResponse",
    ),
    (
        "/tinkle.v1.BotApiService/GetChat",
        "GetChatRequest",
        "GetChatResponse",
    ),
    (
        "/tinkle.v1.BotApiService/GetChatParticipant",
        "GetChatParticipantRequest",
        "GetChatParticipantResponse",
    ),
    (
        "/tinkle.v1.BotApiService/GetChatParticipantCount",
        "GetChatParticipantCountRequest",
        "GetChatParticipantCountResponse",
    ),
    (
        "/tinkle.v1.BotApiService/BanChatParticipant",
        "BanChatParticipantRequest",
        "BanChatParticipantResponse",
    ),
    (
        "/tinkle.v1.BotApiService/UnbanChatParticipant",
        "UnbanChatParticipantRequest",
        "UnbanChatParticipantResponse",
    ),
    (
        "/tinkle.v1.BotApiService/RestrictChatParticipant",
        "RestrictChatParticipantRequest",
        "RestrictChatParticipantResponse",
    ),
    (
        "/tinkle.v1.BotApiService/PromoteChatParticipant",
        "PromoteChatParticipantRequest",
        "PromoteChatParticipantResponse",
    ),
    (
        "/tinkle.v1.BotApiService/LeaveChat",
        "LeaveChatRequest",
        "LeaveChatResponse",
    ),
    (
        "/tinkle.v1.BotApiService/SetWebhook",
        "SetWebhookRequest",
        "SetWebhookResponse",
    ),
    (
        "/tinkle.v1.BotApiService/DeleteWebhook",
        "DeleteWebhookRequest",
        "DeleteWebhookResponse",
    ),
    (
        "/tinkle.v1.BotApiService/GetWebhookInfo",
        "GetWebhookInfoRequest",
        "GetWebhookInfoResponse",
    ),
    (
        "/tinkle.v1.BotApiService/AnswerCallbackQuery",
        "AnswerCallbackQueryRequest",
        "AnswerCallbackQueryResponse",
    ),
    (
        "/tinkle.v1.BotsService/SetCommands",
        "SetCommandsRequest",
        "SetCommandsResponse",
    ),
    (
        "/tinkle.v1.BotsService/GetCommands",
        "GetCommandsRequest",
        "GetCommandsResponse",
    ),
    (
        "/tinkle.v1.BotsService/ResetCommands",
        "ResetCommandsRequest",
        "ResetCommandsResponse",
    ),
    (
        "/tinkle.v1.BotsService/SetMenuButton",
        "SetMenuButtonRequest",
        "SetMenuButtonResponse",
    ),
    (
        "/tinkle.v1.BotsService/GetMenuButton",
        "GetMenuButtonRequest",
        "GetMenuButtonResponse",
    ),
    (
        "/tinkle.v1.BotsService/SetGroupDefaultAdminScopes",
        "SetGroupDefaultAdminScopesRequest",
        "SetGroupDefaultAdminScopesResponse",
    ),
    (
        "/tinkle.v1.BotsService/SetBroadcastDefaultAdminScopes",
        "SetBroadcastDefaultAdminScopesRequest",
        "SetBroadcastDefaultAdminScopesResponse",
    ),
    (
        "/tinkle.v1.BotsService/SetInfo",
        "SetInfoRequest",
        "SetInfoResponse",
    ),
    (
        "/tinkle.v1.BotsService/GetInfo",
        "GetInfoRequest",
        "GetInfoResponse",
    ),
    (
        "/tinkle.v1.BotsService/ToggleUsername",
        "ToggleUsernameRequest",
        "ToggleUsernameResponse",
    ),
    (
        "/tinkle.v1.BotsService/ReorderUsernames",
        "ReorderUsernamesRequest",
        "ReorderUsernamesResponse",
    ),
    (
        "/tinkle.v1.BotsService/GetOwnedBots",
        "GetOwnedBotsRequest",
        "GetOwnedBotsResponse",
    ),
    (
        "/tinkle.v1.BotsService/CanSendMessage",
        "CanSendMessageRequest",
        "CanSendMessageResponse",
    ),
    (
        "/tinkle.v1.BotsService/AllowSendMessage",
        "AllowSendMessageRequest",
        "AllowSendMessageResponse",
    ),
    (
        "/tinkle.v1.BotsService/SendCustomRequest",
        "SendCustomRequestRequest",
        "SendCustomRequestResponse",
    ),
    (
        "/tinkle.v1.BotsService/AnswerWebhookQuery",
        "AnswerWebhookQueryRequest",
        "AnswerWebhookQueryResponse",
    ),
    (
        "/tinkle.v1.ChannelsService/GetChannels",
        "GetChannelsRequest",
        "GetChannelsResponse",
    ),
    (
        "/tinkle.v1.ChannelsService/GetFullChannel",
        "GetFullChannelRequest",
        "GetFullChannelResponse",
    ),
    (
        "/tinkle.v1.ChannelsService/CreateChannel",
        "CreateChannelRequest",
        "CreateChannelResponse",
    ),
    (
        "/tinkle.v1.ChannelsService/DeleteChannel",
        "DeleteChannelRequest",
        "DeleteChannelResponse",
    ),
    (
        "/tinkle.v1.ChannelsService/GetParticipants",
        "GetParticipantsRequest",
        "GetParticipantsResponse",
    ),
    (
        "/tinkle.v1.ChannelsService/GetParticipant",
        "GetParticipantRequest",
        "GetParticipantResponse",
    ),
    (
        "/tinkle.v1.ChannelsService/EditAdmin",
        "EditAdminRequest",
        "EditAdminResponse",
    ),
    (
        "/tinkle.v1.ChannelsService/EditBanned",
        "EditBannedRequest",
        "EditBannedResponse",
    ),
    (
        "/tinkle.v1.ChannelsService/DeleteParticipantHistory",
        "DeleteParticipantHistoryRequest",
        "DeleteParticipantHistoryResponse",
    ),
    (
        "/tinkle.v1.ChannelsService/JoinChannel",
        "JoinChannelRequest",
        "JoinChannelResponse",
    ),
    (
        "/tinkle.v1.ChannelsService/LeaveChannel",
        "LeaveChannelRequest",
        "LeaveChannelResponse",
    ),
    (
        "/tinkle.v1.ChannelsService/InviteToChannel",
        "InviteToChannelRequest",
        "InviteToChannelResponse",
    ),
    (
        "/tinkle.v1.ChannelsService/ToggleParticipantsHidden",
        "ToggleParticipantsHiddenRequest",
        "ToggleParticipantsHiddenResponse",
    ),
    (
        "/tinkle.v1.ChannelsService/TogglePreHistoryHidden",
        "TogglePreHistoryHiddenRequest",
        "TogglePreHistoryHiddenResponse",
    ),
    (
        "/tinkle.v1.ChannelsService/CheckUsername",
        "CheckUsernameRequest",
        "CheckUsernameResponse",
    ),
    (
        "/tinkle.v1.ChannelsService/UpdateUsername",
        "ChannelsServiceUpdateUsernameRequest",
        "ChannelsServiceUpdateUsernameResponse",
    ),
    (
        "/tinkle.v1.ChannelsService/ToggleUsername",
        "ChannelsServiceToggleUsernameRequest",
        "ChannelsServiceToggleUsernameResponse",
    ),
    (
        "/tinkle.v1.ChannelsService/EditTitle",
        "EditTitleRequest",
        "EditTitleResponse",
    ),
    (
        "/tinkle.v1.ChannelsService/EditPhoto",
        "EditPhotoRequest",
        "EditPhotoResponse",
    ),
    (
        "/tinkle.v1.ChannelsService/ToggleSlowMode",
        "ToggleSlowModeRequest",
        "ToggleSlowModeResponse",
    ),
    (
        "/tinkle.v1.ChannelsService/ToggleJoinRequest",
        "ToggleJoinRequestRequest",
        "ToggleJoinRequestResponse",
    ),
    (
        "/tinkle.v1.ChannelsService/GetJoinRequests",
        "GetJoinRequestsRequest",
        "GetJoinRequestsResponse",
    ),
    (
        "/tinkle.v1.ChannelsService/HideJoinRequest",
        "HideJoinRequestRequest",
        "HideJoinRequestResponse",
    ),
    (
        "/tinkle.v1.ChannelsService/HideAllJoinRequests",
        "HideAllJoinRequestsRequest",
        "HideAllJoinRequestsResponse",
    ),
    (
        "/tinkle.v1.ChannelsService/GetChannelMessages",
        "GetChannelMessagesRequest",
        "GetChannelMessagesResponse",
    ),
    (
        "/tinkle.v1.ChannelsService/ReadChannelHistory",
        "ReadChannelHistoryRequest",
        "ReadChannelHistoryResponse",
    ),
    (
        "/tinkle.v1.ChannelsService/DeleteChannelMessages",
        "DeleteChannelMessagesRequest",
        "DeleteChannelMessagesResponse",
    ),
    (
        "/tinkle.v1.ChannelsService/DeleteChannelHistory",
        "DeleteChannelHistoryRequest",
        "DeleteChannelHistoryResponse",
    ),
    (
        "/tinkle.v1.ContactsService/ImportContacts",
        "ImportContactsRequest",
        "ImportContactsResponse",
    ),
    (
        "/tinkle.v1.ContactsService/GetContacts",
        "GetContactsRequest",
        "GetContactsResponse",
    ),
    (
        "/tinkle.v1.ContactsService/GetPresences",
        "GetPresencesRequest",
        "GetPresencesResponse",
    ),
    (
        "/tinkle.v1.ContactsService/AddContact",
        "AddContactRequest",
        "AddContactResponse",
    ),
    (
        "/tinkle.v1.ContactsService/DeleteContacts",
        "DeleteContactsRequest",
        "DeleteContactsResponse",
    ),
    (
        "/tinkle.v1.ContactsService/DeleteByPhones",
        "DeleteByPhonesRequest",
        "DeleteByPhonesResponse",
    ),
    (
        "/tinkle.v1.ContactsService/ResetSaved",
        "ResetSavedRequest",
        "ResetSavedResponse",
    ),
    (
        "/tinkle.v1.ContactsService/Block",
        "BlockRequest",
        "BlockResponse",
    ),
    (
        "/tinkle.v1.ContactsService/Unblock",
        "UnblockRequest",
        "UnblockResponse",
    ),
    (
        "/tinkle.v1.ContactsService/GetBlocked",
        "GetBlockedRequest",
        "GetBlockedResponse",
    ),
    (
        "/tinkle.v1.ContactsService/GetSaved",
        "GetSavedRequest",
        "GetSavedResponse",
    ),
    (
        "/tinkle.v1.HelpService/GetConfig",
        "GetConfigRequest",
        "GetConfigResponse",
    ),
    (
        "/tinkle.v1.HelpService/GetUpdateInfo",
        "GetUpdateInfoRequest",
        "GetUpdateInfoResponse",
    ),
    (
        "/tinkle.v1.HelpService/GetInviteText",
        "GetInviteTextRequest",
        "GetInviteTextResponse",
    ),
    (
        "/tinkle.v1.HelpService/GetSupport",
        "GetSupportRequest",
        "GetSupportResponse",
    ),
    (
        "/tinkle.v1.HelpService/GetAppConfig",
        "GetAppConfigRequest",
        "GetAppConfigResponse",
    ),
    (
        "/tinkle.v1.HelpService/SaveAppLog",
        "SaveAppLogRequest",
        "SaveAppLogResponse",
    ),
    (
        "/tinkle.v1.HelpService/GetTermsOfServiceUpdate",
        "GetTermsOfServiceUpdateRequest",
        "GetTermsOfServiceUpdateResponse",
    ),
    (
        "/tinkle.v1.HelpService/AcceptTermsOfService",
        "AcceptTermsOfServiceRequest",
        "AcceptTermsOfServiceResponse",
    ),
    (
        "/tinkle.v1.HelpService/GetUserInfo",
        "GetUserInfoRequest",
        "GetUserInfoResponse",
    ),
    (
        "/tinkle.v1.HelpService/EditUserInfo",
        "EditUserInfoRequest",
        "EditUserInfoResponse",
    ),
    (
        "/tinkle.v1.HelpService/GetCountryList",
        "GetCountryListRequest",
        "GetCountryListResponse",
    ),
    (
        "/tinkle.v1.HelpService/GetTimezoneList",
        "GetTimezoneListRequest",
        "GetTimezoneListResponse",
    ),
    (
        "/tinkle.v1.MessagesService/GetDialogs",
        "GetDialogsRequest",
        "GetDialogsResponse",
    ),
    (
        "/tinkle.v1.MessagesService/GetHistory",
        "GetHistoryRequest",
        "GetHistoryResponse",
    ),
    (
        "/tinkle.v1.MessagesService/ReadHistory",
        "ReadHistoryRequest",
        "ReadHistoryResponse",
    ),
    (
        "/tinkle.v1.MessagesService/DeleteHistory",
        "DeleteHistoryRequest",
        "DeleteHistoryResponse",
    ),
    (
        "/tinkle.v1.MessagesService/DeleteMessages",
        "DeleteMessagesRequest",
        "DeleteMessagesResponse",
    ),
    (
        "/tinkle.v1.MessagesService/CreateChat",
        "CreateChatRequest",
        "CreateChatResponse",
    ),
    (
        "/tinkle.v1.MessagesService/EditChatTitle",
        "EditChatTitleRequest",
        "EditChatTitleResponse",
    ),
    (
        "/tinkle.v1.MessagesService/EditChatPhoto",
        "EditChatPhotoRequest",
        "EditChatPhotoResponse",
    ),
    (
        "/tinkle.v1.MessagesService/EditChatAbout",
        "EditChatAboutRequest",
        "EditChatAboutResponse",
    ),
    (
        "/tinkle.v1.MessagesService/GetChats",
        "GetChatsRequest",
        "GetChatsResponse",
    ),
    (
        "/tinkle.v1.MessagesService/GetChatComplete",
        "GetChatCompleteRequest",
        "GetChatCompleteResponse",
    ),
    (
        "/tinkle.v1.MessagesService/AddChatParticipant",
        "AddChatParticipantRequest",
        "AddChatParticipantResponse",
    ),
    (
        "/tinkle.v1.MessagesService/RemoveChatParticipant",
        "RemoveChatParticipantRequest",
        "RemoveChatParticipantResponse",
    ),
    (
        "/tinkle.v1.MessagesService/EditChatAdmin",
        "EditChatAdminRequest",
        "EditChatAdminResponse",
    ),
    (
        "/tinkle.v1.MessagesService/EditChatRestrictedScopes",
        "EditChatRestrictedScopesRequest",
        "EditChatRestrictedScopesResponse",
    ),
    (
        "/tinkle.v1.MessagesService/SetHistoryTTL",
        "SetHistoryTTLRequest",
        "SetHistoryTTLResponse",
    ),
    (
        "/tinkle.v1.MessagesService/UpdatePinnedMessage",
        "UpdatePinnedMessageRequest",
        "UpdatePinnedMessageResponse",
    ),
    (
        "/tinkle.v1.MessagesService/UnpinAllMessages",
        "UnpinAllMessagesRequest",
        "UnpinAllMessagesResponse",
    ),
    (
        "/tinkle.v1.MessagesService/SendMessage",
        "SendMessageRequest",
        "SendMessageResponse",
    ),
    (
        "/tinkle.v1.MessagesService/SendMedia",
        "SendMediaRequest",
        "SendMediaResponse",
    ),
    (
        "/tinkle.v1.MessagesService/SendMultiMedia",
        "SendMultiMediaRequest",
        "SendMultiMediaResponse",
    ),
    (
        "/tinkle.v1.MessagesService/EditMessage",
        "EditMessageRequest",
        "EditMessageResponse",
    ),
    (
        "/tinkle.v1.MessagesService/SetTyping",
        "SetTypingRequest",
        "SetTypingResponse",
    ),
    (
        "/tinkle.v1.MessagesService/SendStartBot",
        "SendStartBotRequest",
        "SendStartBotResponse",
    ),
    (
        "/tinkle.v1.MessagesService/Search",
        "SearchRequest",
        "SearchResponse",
    ),
    (
        "/tinkle.v1.MessagesService/GetMessagesReactions",
        "GetMessagesReactionsRequest",
        "GetMessagesReactionsResponse",
    ),
    (
        "/tinkle.v1.MessagesService/GetMessageReactionsList",
        "GetMessageReactionsListRequest",
        "GetMessageReactionsListResponse",
    ),
    (
        "/tinkle.v1.MessagesService/SendReaction",
        "SendReactionRequest",
        "SendReactionResponse",
    ),
    (
        "/tinkle.v1.MessagesService/GetUnreadMentions",
        "GetUnreadMentionsRequest",
        "GetUnreadMentionsResponse",
    ),
    (
        "/tinkle.v1.MessagesService/ReadMentions",
        "ReadMentionsRequest",
        "ReadMentionsResponse",
    ),
    (
        "/tinkle.v1.MessagesService/GetUnreadReactions",
        "GetUnreadReactionsRequest",
        "GetUnreadReactionsResponse",
    ),
    (
        "/tinkle.v1.MessagesService/ReadReactions",
        "ReadReactionsRequest",
        "ReadReactionsResponse",
    ),
    (
        "/tinkle.v1.MessagesService/SendVote",
        "SendVoteRequest",
        "SendVoteResponse",
    ),
    (
        "/tinkle.v1.MessagesService/GetPollVotes",
        "GetPollVotesRequest",
        "GetPollVotesResponse",
    ),
    (
        "/tinkle.v1.MessagesService/GetPollResults",
        "GetPollResultsRequest",
        "GetPollResultsResponse",
    ),
    (
        "/tinkle.v1.MessagesService/ReadThread",
        "ReadThreadRequest",
        "ReadThreadResponse",
    ),
    (
        "/tinkle.v1.MessagesService/GetThreadMessage",
        "GetThreadMessageRequest",
        "GetThreadMessageResponse",
    ),
    (
        "/tinkle.v1.StickersService/GetInstalledPacks",
        "GetInstalledPacksRequest",
        "GetInstalledPacksResponse",
    ),
    (
        "/tinkle.v1.StickersService/InstallPack",
        "InstallPackRequest",
        "InstallPackResponse",
    ),
    (
        "/tinkle.v1.StickersService/UninstallPack",
        "UninstallPackRequest",
        "UninstallPackResponse",
    ),
    (
        "/tinkle.v1.StickersService/ReorderPacks",
        "ReorderPacksRequest",
        "ReorderPacksResponse",
    ),
    (
        "/tinkle.v1.StickersService/GetPack",
        "GetPackRequest",
        "GetPackResponse",
    ),
    (
        "/tinkle.v1.StickersService/GetPackByShortName",
        "GetPackByShortNameRequest",
        "GetPackByShortNameResponse",
    ),
    (
        "/tinkle.v1.StickersService/SearchPacks",
        "SearchPacksRequest",
        "SearchPacksResponse",
    ),
    (
        "/tinkle.v1.StickersService/GetArchivedPacks",
        "GetArchivedPacksRequest",
        "GetArchivedPacksResponse",
    ),
    (
        "/tinkle.v1.StickersService/GetRecentStickers",
        "GetRecentStickersRequest",
        "GetRecentStickersResponse",
    ),
    (
        "/tinkle.v1.StickersService/SaveRecentSticker",
        "SaveRecentStickerRequest",
        "SaveRecentStickerResponse",
    ),
    (
        "/tinkle.v1.StickersService/ClearRecentStickers",
        "ClearRecentStickersRequest",
        "ClearRecentStickersResponse",
    ),
    (
        "/tinkle.v1.StickersService/GetFavoriteStickers",
        "GetFavoriteStickersRequest",
        "GetFavoriteStickersResponse",
    ),
    (
        "/tinkle.v1.StickersService/ToggleFavoriteSticker",
        "ToggleFavoriteStickerRequest",
        "ToggleFavoriteStickerResponse",
    ),
    (
        "/tinkle.v1.UsersService/GetUsers",
        "GetUsersRequest",
        "GetUsersResponse",
    ),
    (
        "/tinkle.v1.UsersService/GetUserComplete",
        "GetUserCompleteRequest",
        "GetUserCompleteResponse",
    ),
];

/// Methods on `/tinkle.v1.AccountService`.
pub mod accountservice {
    /// `/tinkle.v1.AccountService/RegisterPush`: `RegisterPushRequest` → `RegisterPushResponse`.
    pub const REGISTER_PUSH: &str = "/tinkle.v1.AccountService/RegisterPush";
    /// `/tinkle.v1.AccountService/UnregisterPush`: `UnregisterPushRequest` → `UnregisterPushResponse`.
    pub const UNREGISTER_PUSH: &str = "/tinkle.v1.AccountService/UnregisterPush";
    /// `/tinkle.v1.AccountService/GetNotificationSettings`: `GetNotificationSettingsRequest` → `GetNotificationSettingsResponse`.
    pub const GET_NOTIFICATION_SETTINGS: &str = "/tinkle.v1.AccountService/GetNotificationSettings";
    /// `/tinkle.v1.AccountService/UpdateNotificationSetting`: `UpdateNotificationSettingRequest` → `UpdateNotificationSettingResponse`.
    pub const UPDATE_NOTIFICATION_SETTING: &str =
        "/tinkle.v1.AccountService/UpdateNotificationSetting";
    /// `/tinkle.v1.AccountService/ResetNotificationSettings`: `ResetNotificationSettingsRequest` → `ResetNotificationSettingsResponse`.
    pub const RESET_NOTIFICATION_SETTINGS: &str =
        "/tinkle.v1.AccountService/ResetNotificationSettings";
    /// `/tinkle.v1.AccountService/GetPrivacy`: `GetPrivacyRequest` → `GetPrivacyResponse`.
    pub const GET_PRIVACY: &str = "/tinkle.v1.AccountService/GetPrivacy";
    /// `/tinkle.v1.AccountService/SetPrivacy`: `SetPrivacyRequest` → `SetPrivacyResponse`.
    pub const SET_PRIVACY: &str = "/tinkle.v1.AccountService/SetPrivacy";
    /// `/tinkle.v1.AccountService/UpdateProfile`: `UpdateProfileRequest` → `UpdateProfileResponse`.
    pub const UPDATE_PROFILE: &str = "/tinkle.v1.AccountService/UpdateProfile";
    /// `/tinkle.v1.AccountService/ValidateUsername`: `ValidateUsernameRequest` → `ValidateUsernameResponse`.
    pub const VALIDATE_USERNAME: &str = "/tinkle.v1.AccountService/ValidateUsername";
    /// `/tinkle.v1.AccountService/UpdateUsername`: `UpdateUsernameRequest` → `UpdateUsernameResponse`.
    pub const UPDATE_USERNAME: &str = "/tinkle.v1.AccountService/UpdateUsername";
    /// `/tinkle.v1.AccountService/DeleteAccount`: `DeleteAccountRequest` → `DeleteAccountResponse`.
    pub const DELETE_ACCOUNT: &str = "/tinkle.v1.AccountService/DeleteAccount";
    /// `/tinkle.v1.AccountService/SendChangePhoneCode`: `SendChangePhoneCodeRequest` → `SendChangePhoneCodeResponse`.
    pub const SEND_CHANGE_PHONE_CODE: &str = "/tinkle.v1.AccountService/SendChangePhoneCode";
    /// `/tinkle.v1.AccountService/ChangePhoneNumber`: `ChangePhoneNumberRequest` → `ChangePhoneNumberResponse`.
    pub const CHANGE_PHONE_NUMBER: &str = "/tinkle.v1.AccountService/ChangePhoneNumber";
    /// `/tinkle.v1.AccountService/GetContentSettings`: `GetContentSettingsRequest` → `GetContentSettingsResponse`.
    pub const GET_CONTENT_SETTINGS: &str = "/tinkle.v1.AccountService/GetContentSettings";
    /// `/tinkle.v1.AccountService/SetContentSetting`: `SetContentSettingRequest` → `SetContentSettingResponse`.
    pub const SET_CONTENT_SETTING: &str = "/tinkle.v1.AccountService/SetContentSetting";
    /// `/tinkle.v1.AccountService/Logout`: `LogoutRequest` → `LogoutResponse`.
    pub const LOGOUT: &str = "/tinkle.v1.AccountService/Logout";
    /// `/tinkle.v1.AccountService/RevokeSession`: `RevokeSessionRequest` → `RevokeSessionResponse`.
    pub const REVOKE_SESSION: &str = "/tinkle.v1.AccountService/RevokeSession";
    /// `/tinkle.v1.AccountService/RevokeSessions`: `RevokeSessionsRequest` → `RevokeSessionsResponse`.
    pub const REVOKE_SESSIONS: &str = "/tinkle.v1.AccountService/RevokeSessions";
    /// `/tinkle.v1.AccountService/GetSessions`: `GetSessionsRequest` → `GetSessionsResponse`.
    pub const GET_SESSIONS: &str = "/tinkle.v1.AccountService/GetSessions";
}

/// Methods on `/tinkle.v1.AuthService`.
pub mod authservice {
    /// `/tinkle.v1.AuthService/SendCode`: `SendCodeRequest` → `SendCodeResponse`.
    pub const SEND_CODE: &str = "/tinkle.v1.AuthService/SendCode";
    /// `/tinkle.v1.AuthService/SignIn`: `SignInRequest` → `SignInResponse`.
    pub const SIGN_IN: &str = "/tinkle.v1.AuthService/SignIn";
}

/// Methods on `/tinkle.v1.BotApiService`.
pub mod botapiservice {
    /// `/tinkle.v1.BotApiService/GetMe`: `GetMeRequest` → `GetMeResponse`.
    pub const GET_ME: &str = "/tinkle.v1.BotApiService/GetMe";
    /// `/tinkle.v1.BotApiService/SendMessage`: `BotApiServiceSendMessageRequest` → `BotApiServiceSendMessageResponse`.
    pub const SEND_MESSAGE: &str = "/tinkle.v1.BotApiService/SendMessage";
    /// `/tinkle.v1.BotApiService/SendPhoto`: `SendPhotoRequest` → `SendPhotoResponse`.
    pub const SEND_PHOTO: &str = "/tinkle.v1.BotApiService/SendPhoto";
    /// `/tinkle.v1.BotApiService/SendDocument`: `SendDocumentRequest` → `SendDocumentResponse`.
    pub const SEND_DOCUMENT: &str = "/tinkle.v1.BotApiService/SendDocument";
    /// `/tinkle.v1.BotApiService/SendVideo`: `SendVideoRequest` → `SendVideoResponse`.
    pub const SEND_VIDEO: &str = "/tinkle.v1.BotApiService/SendVideo";
    /// `/tinkle.v1.BotApiService/SendAudio`: `SendAudioRequest` → `SendAudioResponse`.
    pub const SEND_AUDIO: &str = "/tinkle.v1.BotApiService/SendAudio";
    /// `/tinkle.v1.BotApiService/SendVoice`: `SendVoiceRequest` → `SendVoiceResponse`.
    pub const SEND_VOICE: &str = "/tinkle.v1.BotApiService/SendVoice";
    /// `/tinkle.v1.BotApiService/SendSticker`: `SendStickerRequest` → `SendStickerResponse`.
    pub const SEND_STICKER: &str = "/tinkle.v1.BotApiService/SendSticker";
    /// `/tinkle.v1.BotApiService/SendLocation`: `SendLocationRequest` → `SendLocationResponse`.
    pub const SEND_LOCATION: &str = "/tinkle.v1.BotApiService/SendLocation";
    /// `/tinkle.v1.BotApiService/SendContact`: `SendContactRequest` → `SendContactResponse`.
    pub const SEND_CONTACT: &str = "/tinkle.v1.BotApiService/SendContact";
    /// `/tinkle.v1.BotApiService/ForwardMessage`: `ForwardMessageRequest` → `ForwardMessageResponse`.
    pub const FORWARD_MESSAGE: &str = "/tinkle.v1.BotApiService/ForwardMessage";
    /// `/tinkle.v1.BotApiService/CopyMessage`: `CopyMessageRequest` → `CopyMessageResponse`.
    pub const COPY_MESSAGE: &str = "/tinkle.v1.BotApiService/CopyMessage";
    /// `/tinkle.v1.BotApiService/EditMessageText`: `EditMessageTextRequest` → `EditMessageTextResponse`.
    pub const EDIT_MESSAGE_TEXT: &str = "/tinkle.v1.BotApiService/EditMessageText";
    /// `/tinkle.v1.BotApiService/EditMessageCaption`: `EditMessageCaptionRequest` → `EditMessageCaptionResponse`.
    pub const EDIT_MESSAGE_CAPTION: &str = "/tinkle.v1.BotApiService/EditMessageCaption";
    /// `/tinkle.v1.BotApiService/EditMessageMedia`: `EditMessageMediaRequest` → `EditMessageMediaResponse`.
    pub const EDIT_MESSAGE_MEDIA: &str = "/tinkle.v1.BotApiService/EditMessageMedia";
    /// `/tinkle.v1.BotApiService/EditMessageReplyMarkup`: `EditMessageReplyMarkupRequest` → `EditMessageReplyMarkupResponse`.
    pub const EDIT_MESSAGE_REPLY_MARKUP: &str = "/tinkle.v1.BotApiService/EditMessageReplyMarkup";
    /// `/tinkle.v1.BotApiService/DeleteMessage`: `DeleteMessageRequest` → `DeleteMessageResponse`.
    pub const DELETE_MESSAGE: &str = "/tinkle.v1.BotApiService/DeleteMessage";
    /// `/tinkle.v1.BotApiService/GetChat`: `GetChatRequest` → `GetChatResponse`.
    pub const GET_CHAT: &str = "/tinkle.v1.BotApiService/GetChat";
    /// `/tinkle.v1.BotApiService/GetChatParticipant`: `GetChatParticipantRequest` → `GetChatParticipantResponse`.
    pub const GET_CHAT_PARTICIPANT: &str = "/tinkle.v1.BotApiService/GetChatParticipant";
    /// `/tinkle.v1.BotApiService/GetChatParticipantCount`: `GetChatParticipantCountRequest` → `GetChatParticipantCountResponse`.
    pub const GET_CHAT_PARTICIPANT_COUNT: &str = "/tinkle.v1.BotApiService/GetChatParticipantCount";
    /// `/tinkle.v1.BotApiService/BanChatParticipant`: `BanChatParticipantRequest` → `BanChatParticipantResponse`.
    pub const BAN_CHAT_PARTICIPANT: &str = "/tinkle.v1.BotApiService/BanChatParticipant";
    /// `/tinkle.v1.BotApiService/UnbanChatParticipant`: `UnbanChatParticipantRequest` → `UnbanChatParticipantResponse`.
    pub const UNBAN_CHAT_PARTICIPANT: &str = "/tinkle.v1.BotApiService/UnbanChatParticipant";
    /// `/tinkle.v1.BotApiService/RestrictChatParticipant`: `RestrictChatParticipantRequest` → `RestrictChatParticipantResponse`.
    pub const RESTRICT_CHAT_PARTICIPANT: &str = "/tinkle.v1.BotApiService/RestrictChatParticipant";
    /// `/tinkle.v1.BotApiService/PromoteChatParticipant`: `PromoteChatParticipantRequest` → `PromoteChatParticipantResponse`.
    pub const PROMOTE_CHAT_PARTICIPANT: &str = "/tinkle.v1.BotApiService/PromoteChatParticipant";
    /// `/tinkle.v1.BotApiService/LeaveChat`: `LeaveChatRequest` → `LeaveChatResponse`.
    pub const LEAVE_CHAT: &str = "/tinkle.v1.BotApiService/LeaveChat";
    /// `/tinkle.v1.BotApiService/SetWebhook`: `SetWebhookRequest` → `SetWebhookResponse`.
    pub const SET_WEBHOOK: &str = "/tinkle.v1.BotApiService/SetWebhook";
    /// `/tinkle.v1.BotApiService/DeleteWebhook`: `DeleteWebhookRequest` → `DeleteWebhookResponse`.
    pub const DELETE_WEBHOOK: &str = "/tinkle.v1.BotApiService/DeleteWebhook";
    /// `/tinkle.v1.BotApiService/GetWebhookInfo`: `GetWebhookInfoRequest` → `GetWebhookInfoResponse`.
    pub const GET_WEBHOOK_INFO: &str = "/tinkle.v1.BotApiService/GetWebhookInfo";
    /// `/tinkle.v1.BotApiService/AnswerCallbackQuery`: `AnswerCallbackQueryRequest` → `AnswerCallbackQueryResponse`.
    pub const ANSWER_CALLBACK_QUERY: &str = "/tinkle.v1.BotApiService/AnswerCallbackQuery";
}

/// Methods on `/tinkle.v1.BotsService`.
pub mod botsservice {
    /// `/tinkle.v1.BotsService/SetCommands`: `SetCommandsRequest` → `SetCommandsResponse`.
    pub const SET_COMMANDS: &str = "/tinkle.v1.BotsService/SetCommands";
    /// `/tinkle.v1.BotsService/GetCommands`: `GetCommandsRequest` → `GetCommandsResponse`.
    pub const GET_COMMANDS: &str = "/tinkle.v1.BotsService/GetCommands";
    /// `/tinkle.v1.BotsService/ResetCommands`: `ResetCommandsRequest` → `ResetCommandsResponse`.
    pub const RESET_COMMANDS: &str = "/tinkle.v1.BotsService/ResetCommands";
    /// `/tinkle.v1.BotsService/SetMenuButton`: `SetMenuButtonRequest` → `SetMenuButtonResponse`.
    pub const SET_MENU_BUTTON: &str = "/tinkle.v1.BotsService/SetMenuButton";
    /// `/tinkle.v1.BotsService/GetMenuButton`: `GetMenuButtonRequest` → `GetMenuButtonResponse`.
    pub const GET_MENU_BUTTON: &str = "/tinkle.v1.BotsService/GetMenuButton";
    /// `/tinkle.v1.BotsService/SetGroupDefaultAdminScopes`: `SetGroupDefaultAdminScopesRequest` → `SetGroupDefaultAdminScopesResponse`.
    pub const SET_GROUP_DEFAULT_ADMIN_SCOPES: &str =
        "/tinkle.v1.BotsService/SetGroupDefaultAdminScopes";
    /// `/tinkle.v1.BotsService/SetBroadcastDefaultAdminScopes`: `SetBroadcastDefaultAdminScopesRequest` → `SetBroadcastDefaultAdminScopesResponse`.
    pub const SET_BROADCAST_DEFAULT_ADMIN_SCOPES: &str =
        "/tinkle.v1.BotsService/SetBroadcastDefaultAdminScopes";
    /// `/tinkle.v1.BotsService/SetInfo`: `SetInfoRequest` → `SetInfoResponse`.
    pub const SET_INFO: &str = "/tinkle.v1.BotsService/SetInfo";
    /// `/tinkle.v1.BotsService/GetInfo`: `GetInfoRequest` → `GetInfoResponse`.
    pub const GET_INFO: &str = "/tinkle.v1.BotsService/GetInfo";
    /// `/tinkle.v1.BotsService/ToggleUsername`: `ToggleUsernameRequest` → `ToggleUsernameResponse`.
    pub const TOGGLE_USERNAME: &str = "/tinkle.v1.BotsService/ToggleUsername";
    /// `/tinkle.v1.BotsService/ReorderUsernames`: `ReorderUsernamesRequest` → `ReorderUsernamesResponse`.
    pub const REORDER_USERNAMES: &str = "/tinkle.v1.BotsService/ReorderUsernames";
    /// `/tinkle.v1.BotsService/GetOwnedBots`: `GetOwnedBotsRequest` → `GetOwnedBotsResponse`.
    pub const GET_OWNED_BOTS: &str = "/tinkle.v1.BotsService/GetOwnedBots";
    /// `/tinkle.v1.BotsService/CanSendMessage`: `CanSendMessageRequest` → `CanSendMessageResponse`.
    pub const CAN_SEND_MESSAGE: &str = "/tinkle.v1.BotsService/CanSendMessage";
    /// `/tinkle.v1.BotsService/AllowSendMessage`: `AllowSendMessageRequest` → `AllowSendMessageResponse`.
    pub const ALLOW_SEND_MESSAGE: &str = "/tinkle.v1.BotsService/AllowSendMessage";
    /// `/tinkle.v1.BotsService/SendCustomRequest`: `SendCustomRequestRequest` → `SendCustomRequestResponse`.
    pub const SEND_CUSTOM_REQUEST: &str = "/tinkle.v1.BotsService/SendCustomRequest";
    /// `/tinkle.v1.BotsService/AnswerWebhookQuery`: `AnswerWebhookQueryRequest` → `AnswerWebhookQueryResponse`.
    pub const ANSWER_WEBHOOK_QUERY: &str = "/tinkle.v1.BotsService/AnswerWebhookQuery";
}

/// Methods on `/tinkle.v1.ChannelsService`.
pub mod channelsservice {
    /// `/tinkle.v1.ChannelsService/GetChannels`: `GetChannelsRequest` → `GetChannelsResponse`.
    pub const GET_CHANNELS: &str = "/tinkle.v1.ChannelsService/GetChannels";
    /// `/tinkle.v1.ChannelsService/GetFullChannel`: `GetFullChannelRequest` → `GetFullChannelResponse`.
    pub const GET_FULL_CHANNEL: &str = "/tinkle.v1.ChannelsService/GetFullChannel";
    /// `/tinkle.v1.ChannelsService/CreateChannel`: `CreateChannelRequest` → `CreateChannelResponse`.
    pub const CREATE_CHANNEL: &str = "/tinkle.v1.ChannelsService/CreateChannel";
    /// `/tinkle.v1.ChannelsService/DeleteChannel`: `DeleteChannelRequest` → `DeleteChannelResponse`.
    pub const DELETE_CHANNEL: &str = "/tinkle.v1.ChannelsService/DeleteChannel";
    /// `/tinkle.v1.ChannelsService/GetParticipants`: `GetParticipantsRequest` → `GetParticipantsResponse`.
    pub const GET_PARTICIPANTS: &str = "/tinkle.v1.ChannelsService/GetParticipants";
    /// `/tinkle.v1.ChannelsService/GetParticipant`: `GetParticipantRequest` → `GetParticipantResponse`.
    pub const GET_PARTICIPANT: &str = "/tinkle.v1.ChannelsService/GetParticipant";
    /// `/tinkle.v1.ChannelsService/EditAdmin`: `EditAdminRequest` → `EditAdminResponse`.
    pub const EDIT_ADMIN: &str = "/tinkle.v1.ChannelsService/EditAdmin";
    /// `/tinkle.v1.ChannelsService/EditBanned`: `EditBannedRequest` → `EditBannedResponse`.
    pub const EDIT_BANNED: &str = "/tinkle.v1.ChannelsService/EditBanned";
    /// `/tinkle.v1.ChannelsService/DeleteParticipantHistory`: `DeleteParticipantHistoryRequest` → `DeleteParticipantHistoryResponse`.
    pub const DELETE_PARTICIPANT_HISTORY: &str =
        "/tinkle.v1.ChannelsService/DeleteParticipantHistory";
    /// `/tinkle.v1.ChannelsService/JoinChannel`: `JoinChannelRequest` → `JoinChannelResponse`.
    pub const JOIN_CHANNEL: &str = "/tinkle.v1.ChannelsService/JoinChannel";
    /// `/tinkle.v1.ChannelsService/LeaveChannel`: `LeaveChannelRequest` → `LeaveChannelResponse`.
    pub const LEAVE_CHANNEL: &str = "/tinkle.v1.ChannelsService/LeaveChannel";
    /// `/tinkle.v1.ChannelsService/InviteToChannel`: `InviteToChannelRequest` → `InviteToChannelResponse`.
    pub const INVITE_TO_CHANNEL: &str = "/tinkle.v1.ChannelsService/InviteToChannel";
    /// `/tinkle.v1.ChannelsService/ToggleParticipantsHidden`: `ToggleParticipantsHiddenRequest` → `ToggleParticipantsHiddenResponse`.
    pub const TOGGLE_PARTICIPANTS_HIDDEN: &str =
        "/tinkle.v1.ChannelsService/ToggleParticipantsHidden";
    /// `/tinkle.v1.ChannelsService/TogglePreHistoryHidden`: `TogglePreHistoryHiddenRequest` → `TogglePreHistoryHiddenResponse`.
    pub const TOGGLE_PRE_HISTORY_HIDDEN: &str = "/tinkle.v1.ChannelsService/TogglePreHistoryHidden";
    /// `/tinkle.v1.ChannelsService/CheckUsername`: `CheckUsernameRequest` → `CheckUsernameResponse`.
    pub const CHECK_USERNAME: &str = "/tinkle.v1.ChannelsService/CheckUsername";
    /// `/tinkle.v1.ChannelsService/UpdateUsername`: `ChannelsServiceUpdateUsernameRequest` → `ChannelsServiceUpdateUsernameResponse`.
    pub const UPDATE_USERNAME: &str = "/tinkle.v1.ChannelsService/UpdateUsername";
    /// `/tinkle.v1.ChannelsService/ToggleUsername`: `ChannelsServiceToggleUsernameRequest` → `ChannelsServiceToggleUsernameResponse`.
    pub const TOGGLE_USERNAME: &str = "/tinkle.v1.ChannelsService/ToggleUsername";
    /// `/tinkle.v1.ChannelsService/EditTitle`: `EditTitleRequest` → `EditTitleResponse`.
    pub const EDIT_TITLE: &str = "/tinkle.v1.ChannelsService/EditTitle";
    /// `/tinkle.v1.ChannelsService/EditPhoto`: `EditPhotoRequest` → `EditPhotoResponse`.
    pub const EDIT_PHOTO: &str = "/tinkle.v1.ChannelsService/EditPhoto";
    /// `/tinkle.v1.ChannelsService/ToggleSlowMode`: `ToggleSlowModeRequest` → `ToggleSlowModeResponse`.
    pub const TOGGLE_SLOW_MODE: &str = "/tinkle.v1.ChannelsService/ToggleSlowMode";
    /// `/tinkle.v1.ChannelsService/ToggleJoinRequest`: `ToggleJoinRequestRequest` → `ToggleJoinRequestResponse`.
    pub const TOGGLE_JOIN_REQUEST: &str = "/tinkle.v1.ChannelsService/ToggleJoinRequest";
    /// `/tinkle.v1.ChannelsService/GetJoinRequests`: `GetJoinRequestsRequest` → `GetJoinRequestsResponse`.
    pub const GET_JOIN_REQUESTS: &str = "/tinkle.v1.ChannelsService/GetJoinRequests";
    /// `/tinkle.v1.ChannelsService/HideJoinRequest`: `HideJoinRequestRequest` → `HideJoinRequestResponse`.
    pub const HIDE_JOIN_REQUEST: &str = "/tinkle.v1.ChannelsService/HideJoinRequest";
    /// `/tinkle.v1.ChannelsService/HideAllJoinRequests`: `HideAllJoinRequestsRequest` → `HideAllJoinRequestsResponse`.
    pub const HIDE_ALL_JOIN_REQUESTS: &str = "/tinkle.v1.ChannelsService/HideAllJoinRequests";
    /// `/tinkle.v1.ChannelsService/GetChannelMessages`: `GetChannelMessagesRequest` → `GetChannelMessagesResponse`.
    pub const GET_CHANNEL_MESSAGES: &str = "/tinkle.v1.ChannelsService/GetChannelMessages";
    /// `/tinkle.v1.ChannelsService/ReadChannelHistory`: `ReadChannelHistoryRequest` → `ReadChannelHistoryResponse`.
    pub const READ_CHANNEL_HISTORY: &str = "/tinkle.v1.ChannelsService/ReadChannelHistory";
    /// `/tinkle.v1.ChannelsService/DeleteChannelMessages`: `DeleteChannelMessagesRequest` → `DeleteChannelMessagesResponse`.
    pub const DELETE_CHANNEL_MESSAGES: &str = "/tinkle.v1.ChannelsService/DeleteChannelMessages";
    /// `/tinkle.v1.ChannelsService/DeleteChannelHistory`: `DeleteChannelHistoryRequest` → `DeleteChannelHistoryResponse`.
    pub const DELETE_CHANNEL_HISTORY: &str = "/tinkle.v1.ChannelsService/DeleteChannelHistory";
}

/// Methods on `/tinkle.v1.ContactsService`.
pub mod contactsservice {
    /// `/tinkle.v1.ContactsService/ImportContacts`: `ImportContactsRequest` → `ImportContactsResponse`.
    pub const IMPORT_CONTACTS: &str = "/tinkle.v1.ContactsService/ImportContacts";
    /// `/tinkle.v1.ContactsService/GetContacts`: `GetContactsRequest` → `GetContactsResponse`.
    pub const GET_CONTACTS: &str = "/tinkle.v1.ContactsService/GetContacts";
    /// `/tinkle.v1.ContactsService/GetPresences`: `GetPresencesRequest` → `GetPresencesResponse`.
    pub const GET_PRESENCES: &str = "/tinkle.v1.ContactsService/GetPresences";
    /// `/tinkle.v1.ContactsService/AddContact`: `AddContactRequest` → `AddContactResponse`.
    pub const ADD_CONTACT: &str = "/tinkle.v1.ContactsService/AddContact";
    /// `/tinkle.v1.ContactsService/DeleteContacts`: `DeleteContactsRequest` → `DeleteContactsResponse`.
    pub const DELETE_CONTACTS: &str = "/tinkle.v1.ContactsService/DeleteContacts";
    /// `/tinkle.v1.ContactsService/DeleteByPhones`: `DeleteByPhonesRequest` → `DeleteByPhonesResponse`.
    pub const DELETE_BY_PHONES: &str = "/tinkle.v1.ContactsService/DeleteByPhones";
    /// `/tinkle.v1.ContactsService/ResetSaved`: `ResetSavedRequest` → `ResetSavedResponse`.
    pub const RESET_SAVED: &str = "/tinkle.v1.ContactsService/ResetSaved";
    /// `/tinkle.v1.ContactsService/Block`: `BlockRequest` → `BlockResponse`.
    pub const BLOCK: &str = "/tinkle.v1.ContactsService/Block";
    /// `/tinkle.v1.ContactsService/Unblock`: `UnblockRequest` → `UnblockResponse`.
    pub const UNBLOCK: &str = "/tinkle.v1.ContactsService/Unblock";
    /// `/tinkle.v1.ContactsService/GetBlocked`: `GetBlockedRequest` → `GetBlockedResponse`.
    pub const GET_BLOCKED: &str = "/tinkle.v1.ContactsService/GetBlocked";
    /// `/tinkle.v1.ContactsService/GetSaved`: `GetSavedRequest` → `GetSavedResponse`.
    pub const GET_SAVED: &str = "/tinkle.v1.ContactsService/GetSaved";
}

/// Methods on `/tinkle.v1.HelpService`.
pub mod helpservice {
    /// `/tinkle.v1.HelpService/GetConfig`: `GetConfigRequest` → `GetConfigResponse`.
    pub const GET_CONFIG: &str = "/tinkle.v1.HelpService/GetConfig";
    /// `/tinkle.v1.HelpService/GetUpdateInfo`: `GetUpdateInfoRequest` → `GetUpdateInfoResponse`.
    pub const GET_UPDATE_INFO: &str = "/tinkle.v1.HelpService/GetUpdateInfo";
    /// `/tinkle.v1.HelpService/GetInviteText`: `GetInviteTextRequest` → `GetInviteTextResponse`.
    pub const GET_INVITE_TEXT: &str = "/tinkle.v1.HelpService/GetInviteText";
    /// `/tinkle.v1.HelpService/GetSupport`: `GetSupportRequest` → `GetSupportResponse`.
    pub const GET_SUPPORT: &str = "/tinkle.v1.HelpService/GetSupport";
    /// `/tinkle.v1.HelpService/GetAppConfig`: `GetAppConfigRequest` → `GetAppConfigResponse`.
    pub const GET_APP_CONFIG: &str = "/tinkle.v1.HelpService/GetAppConfig";
    /// `/tinkle.v1.HelpService/SaveAppLog`: `SaveAppLogRequest` → `SaveAppLogResponse`.
    pub const SAVE_APP_LOG: &str = "/tinkle.v1.HelpService/SaveAppLog";
    /// `/tinkle.v1.HelpService/GetTermsOfServiceUpdate`: `GetTermsOfServiceUpdateRequest` → `GetTermsOfServiceUpdateResponse`.
    pub const GET_TERMS_OF_SERVICE_UPDATE: &str = "/tinkle.v1.HelpService/GetTermsOfServiceUpdate";
    /// `/tinkle.v1.HelpService/AcceptTermsOfService`: `AcceptTermsOfServiceRequest` → `AcceptTermsOfServiceResponse`.
    pub const ACCEPT_TERMS_OF_SERVICE: &str = "/tinkle.v1.HelpService/AcceptTermsOfService";
    /// `/tinkle.v1.HelpService/GetUserInfo`: `GetUserInfoRequest` → `GetUserInfoResponse`.
    pub const GET_USER_INFO: &str = "/tinkle.v1.HelpService/GetUserInfo";
    /// `/tinkle.v1.HelpService/EditUserInfo`: `EditUserInfoRequest` → `EditUserInfoResponse`.
    pub const EDIT_USER_INFO: &str = "/tinkle.v1.HelpService/EditUserInfo";
    /// `/tinkle.v1.HelpService/GetCountryList`: `GetCountryListRequest` → `GetCountryListResponse`.
    pub const GET_COUNTRY_LIST: &str = "/tinkle.v1.HelpService/GetCountryList";
    /// `/tinkle.v1.HelpService/GetTimezoneList`: `GetTimezoneListRequest` → `GetTimezoneListResponse`.
    pub const GET_TIMEZONE_LIST: &str = "/tinkle.v1.HelpService/GetTimezoneList";
}

/// Methods on `/tinkle.v1.MessagesService`.
pub mod messagesservice {
    /// `/tinkle.v1.MessagesService/GetDialogs`: `GetDialogsRequest` → `GetDialogsResponse`.
    pub const GET_DIALOGS: &str = "/tinkle.v1.MessagesService/GetDialogs";
    /// `/tinkle.v1.MessagesService/GetHistory`: `GetHistoryRequest` → `GetHistoryResponse`.
    pub const GET_HISTORY: &str = "/tinkle.v1.MessagesService/GetHistory";
    /// `/tinkle.v1.MessagesService/ReadHistory`: `ReadHistoryRequest` → `ReadHistoryResponse`.
    pub const READ_HISTORY: &str = "/tinkle.v1.MessagesService/ReadHistory";
    /// `/tinkle.v1.MessagesService/DeleteHistory`: `DeleteHistoryRequest` → `DeleteHistoryResponse`.
    pub const DELETE_HISTORY: &str = "/tinkle.v1.MessagesService/DeleteHistory";
    /// `/tinkle.v1.MessagesService/DeleteMessages`: `DeleteMessagesRequest` → `DeleteMessagesResponse`.
    pub const DELETE_MESSAGES: &str = "/tinkle.v1.MessagesService/DeleteMessages";
    /// `/tinkle.v1.MessagesService/CreateChat`: `CreateChatRequest` → `CreateChatResponse`.
    pub const CREATE_CHAT: &str = "/tinkle.v1.MessagesService/CreateChat";
    /// `/tinkle.v1.MessagesService/EditChatTitle`: `EditChatTitleRequest` → `EditChatTitleResponse`.
    pub const EDIT_CHAT_TITLE: &str = "/tinkle.v1.MessagesService/EditChatTitle";
    /// `/tinkle.v1.MessagesService/EditChatPhoto`: `EditChatPhotoRequest` → `EditChatPhotoResponse`.
    pub const EDIT_CHAT_PHOTO: &str = "/tinkle.v1.MessagesService/EditChatPhoto";
    /// `/tinkle.v1.MessagesService/EditChatAbout`: `EditChatAboutRequest` → `EditChatAboutResponse`.
    pub const EDIT_CHAT_ABOUT: &str = "/tinkle.v1.MessagesService/EditChatAbout";
    /// `/tinkle.v1.MessagesService/GetChats`: `GetChatsRequest` → `GetChatsResponse`.
    pub const GET_CHATS: &str = "/tinkle.v1.MessagesService/GetChats";
    /// `/tinkle.v1.MessagesService/GetChatComplete`: `GetChatCompleteRequest` → `GetChatCompleteResponse`.
    pub const GET_CHAT_COMPLETE: &str = "/tinkle.v1.MessagesService/GetChatComplete";
    /// `/tinkle.v1.MessagesService/AddChatParticipant`: `AddChatParticipantRequest` → `AddChatParticipantResponse`.
    pub const ADD_CHAT_PARTICIPANT: &str = "/tinkle.v1.MessagesService/AddChatParticipant";
    /// `/tinkle.v1.MessagesService/RemoveChatParticipant`: `RemoveChatParticipantRequest` → `RemoveChatParticipantResponse`.
    pub const REMOVE_CHAT_PARTICIPANT: &str = "/tinkle.v1.MessagesService/RemoveChatParticipant";
    /// `/tinkle.v1.MessagesService/EditChatAdmin`: `EditChatAdminRequest` → `EditChatAdminResponse`.
    pub const EDIT_CHAT_ADMIN: &str = "/tinkle.v1.MessagesService/EditChatAdmin";
    /// `/tinkle.v1.MessagesService/EditChatRestrictedScopes`: `EditChatRestrictedScopesRequest` → `EditChatRestrictedScopesResponse`.
    pub const EDIT_CHAT_RESTRICTED_SCOPES: &str =
        "/tinkle.v1.MessagesService/EditChatRestrictedScopes";
    /// `/tinkle.v1.MessagesService/SetHistoryTTL`: `SetHistoryTTLRequest` → `SetHistoryTTLResponse`.
    pub const SET_HISTORY_T_T_L: &str = "/tinkle.v1.MessagesService/SetHistoryTTL";
    /// `/tinkle.v1.MessagesService/UpdatePinnedMessage`: `UpdatePinnedMessageRequest` → `UpdatePinnedMessageResponse`.
    pub const UPDATE_PINNED_MESSAGE: &str = "/tinkle.v1.MessagesService/UpdatePinnedMessage";
    /// `/tinkle.v1.MessagesService/UnpinAllMessages`: `UnpinAllMessagesRequest` → `UnpinAllMessagesResponse`.
    pub const UNPIN_ALL_MESSAGES: &str = "/tinkle.v1.MessagesService/UnpinAllMessages";
    /// `/tinkle.v1.MessagesService/SendMessage`: `SendMessageRequest` → `SendMessageResponse`.
    pub const SEND_MESSAGE: &str = "/tinkle.v1.MessagesService/SendMessage";
    /// `/tinkle.v1.MessagesService/SendMedia`: `SendMediaRequest` → `SendMediaResponse`.
    pub const SEND_MEDIA: &str = "/tinkle.v1.MessagesService/SendMedia";
    /// `/tinkle.v1.MessagesService/SendMultiMedia`: `SendMultiMediaRequest` → `SendMultiMediaResponse`.
    pub const SEND_MULTI_MEDIA: &str = "/tinkle.v1.MessagesService/SendMultiMedia";
    /// `/tinkle.v1.MessagesService/EditMessage`: `EditMessageRequest` → `EditMessageResponse`.
    pub const EDIT_MESSAGE: &str = "/tinkle.v1.MessagesService/EditMessage";
    /// `/tinkle.v1.MessagesService/SetTyping`: `SetTypingRequest` → `SetTypingResponse`.
    pub const SET_TYPING: &str = "/tinkle.v1.MessagesService/SetTyping";
    /// `/tinkle.v1.MessagesService/SendStartBot`: `SendStartBotRequest` → `SendStartBotResponse`.
    pub const SEND_START_BOT: &str = "/tinkle.v1.MessagesService/SendStartBot";
    /// `/tinkle.v1.MessagesService/Search`: `SearchRequest` → `SearchResponse`.
    pub const SEARCH: &str = "/tinkle.v1.MessagesService/Search";
    /// `/tinkle.v1.MessagesService/GetMessagesReactions`: `GetMessagesReactionsRequest` → `GetMessagesReactionsResponse`.
    pub const GET_MESSAGES_REACTIONS: &str = "/tinkle.v1.MessagesService/GetMessagesReactions";
    /// `/tinkle.v1.MessagesService/GetMessageReactionsList`: `GetMessageReactionsListRequest` → `GetMessageReactionsListResponse`.
    pub const GET_MESSAGE_REACTIONS_LIST: &str =
        "/tinkle.v1.MessagesService/GetMessageReactionsList";
    /// `/tinkle.v1.MessagesService/SendReaction`: `SendReactionRequest` → `SendReactionResponse`.
    pub const SEND_REACTION: &str = "/tinkle.v1.MessagesService/SendReaction";
    /// `/tinkle.v1.MessagesService/GetUnreadMentions`: `GetUnreadMentionsRequest` → `GetUnreadMentionsResponse`.
    pub const GET_UNREAD_MENTIONS: &str = "/tinkle.v1.MessagesService/GetUnreadMentions";
    /// `/tinkle.v1.MessagesService/ReadMentions`: `ReadMentionsRequest` → `ReadMentionsResponse`.
    pub const READ_MENTIONS: &str = "/tinkle.v1.MessagesService/ReadMentions";
    /// `/tinkle.v1.MessagesService/GetUnreadReactions`: `GetUnreadReactionsRequest` → `GetUnreadReactionsResponse`.
    pub const GET_UNREAD_REACTIONS: &str = "/tinkle.v1.MessagesService/GetUnreadReactions";
    /// `/tinkle.v1.MessagesService/ReadReactions`: `ReadReactionsRequest` → `ReadReactionsResponse`.
    pub const READ_REACTIONS: &str = "/tinkle.v1.MessagesService/ReadReactions";
    /// `/tinkle.v1.MessagesService/SendVote`: `SendVoteRequest` → `SendVoteResponse`.
    pub const SEND_VOTE: &str = "/tinkle.v1.MessagesService/SendVote";
    /// `/tinkle.v1.MessagesService/GetPollVotes`: `GetPollVotesRequest` → `GetPollVotesResponse`.
    pub const GET_POLL_VOTES: &str = "/tinkle.v1.MessagesService/GetPollVotes";
    /// `/tinkle.v1.MessagesService/GetPollResults`: `GetPollResultsRequest` → `GetPollResultsResponse`.
    pub const GET_POLL_RESULTS: &str = "/tinkle.v1.MessagesService/GetPollResults";
    /// `/tinkle.v1.MessagesService/ReadThread`: `ReadThreadRequest` → `ReadThreadResponse`.
    pub const READ_THREAD: &str = "/tinkle.v1.MessagesService/ReadThread";
    /// `/tinkle.v1.MessagesService/GetThreadMessage`: `GetThreadMessageRequest` → `GetThreadMessageResponse`.
    pub const GET_THREAD_MESSAGE: &str = "/tinkle.v1.MessagesService/GetThreadMessage";
}

/// Methods on `/tinkle.v1.StickersService`.
pub mod stickersservice {
    /// `/tinkle.v1.StickersService/GetInstalledPacks`: `GetInstalledPacksRequest` → `GetInstalledPacksResponse`.
    pub const GET_INSTALLED_PACKS: &str = "/tinkle.v1.StickersService/GetInstalledPacks";
    /// `/tinkle.v1.StickersService/InstallPack`: `InstallPackRequest` → `InstallPackResponse`.
    pub const INSTALL_PACK: &str = "/tinkle.v1.StickersService/InstallPack";
    /// `/tinkle.v1.StickersService/UninstallPack`: `UninstallPackRequest` → `UninstallPackResponse`.
    pub const UNINSTALL_PACK: &str = "/tinkle.v1.StickersService/UninstallPack";
    /// `/tinkle.v1.StickersService/ReorderPacks`: `ReorderPacksRequest` → `ReorderPacksResponse`.
    pub const REORDER_PACKS: &str = "/tinkle.v1.StickersService/ReorderPacks";
    /// `/tinkle.v1.StickersService/GetPack`: `GetPackRequest` → `GetPackResponse`.
    pub const GET_PACK: &str = "/tinkle.v1.StickersService/GetPack";
    /// `/tinkle.v1.StickersService/GetPackByShortName`: `GetPackByShortNameRequest` → `GetPackByShortNameResponse`.
    pub const GET_PACK_BY_SHORT_NAME: &str = "/tinkle.v1.StickersService/GetPackByShortName";
    /// `/tinkle.v1.StickersService/SearchPacks`: `SearchPacksRequest` → `SearchPacksResponse`.
    pub const SEARCH_PACKS: &str = "/tinkle.v1.StickersService/SearchPacks";
    /// `/tinkle.v1.StickersService/GetArchivedPacks`: `GetArchivedPacksRequest` → `GetArchivedPacksResponse`.
    pub const GET_ARCHIVED_PACKS: &str = "/tinkle.v1.StickersService/GetArchivedPacks";
    /// `/tinkle.v1.StickersService/GetRecentStickers`: `GetRecentStickersRequest` → `GetRecentStickersResponse`.
    pub const GET_RECENT_STICKERS: &str = "/tinkle.v1.StickersService/GetRecentStickers";
    /// `/tinkle.v1.StickersService/SaveRecentSticker`: `SaveRecentStickerRequest` → `SaveRecentStickerResponse`.
    pub const SAVE_RECENT_STICKER: &str = "/tinkle.v1.StickersService/SaveRecentSticker";
    /// `/tinkle.v1.StickersService/ClearRecentStickers`: `ClearRecentStickersRequest` → `ClearRecentStickersResponse`.
    pub const CLEAR_RECENT_STICKERS: &str = "/tinkle.v1.StickersService/ClearRecentStickers";
    /// `/tinkle.v1.StickersService/GetFavoriteStickers`: `GetFavoriteStickersRequest` → `GetFavoriteStickersResponse`.
    pub const GET_FAVORITE_STICKERS: &str = "/tinkle.v1.StickersService/GetFavoriteStickers";
    /// `/tinkle.v1.StickersService/ToggleFavoriteSticker`: `ToggleFavoriteStickerRequest` → `ToggleFavoriteStickerResponse`.
    pub const TOGGLE_FAVORITE_STICKER: &str = "/tinkle.v1.StickersService/ToggleFavoriteSticker";
}

/// Methods on `/tinkle.v1.UsersService`.
pub mod usersservice {
    /// `/tinkle.v1.UsersService/GetUsers`: `GetUsersRequest` → `GetUsersResponse`.
    pub const GET_USERS: &str = "/tinkle.v1.UsersService/GetUsers";
    /// `/tinkle.v1.UsersService/GetUserComplete`: `GetUserCompleteRequest` → `GetUserCompleteResponse`.
    pub const GET_USER_COMPLETE: &str = "/tinkle.v1.UsersService/GetUserComplete";
}

/// Realtime batched binding (Syntra dispatcher identifiers, not gRPC).
/// See `REALTIME.md` and `realtime.proto`.
pub mod realtime {
    /// `/tinkle.v1.Realtime/SubscribeBatched`: `SubscribeRequest` → `SubscribeResponse`.
    pub const SUBSCRIBE_BATCHED: &str = "/tinkle.v1.Realtime/SubscribeBatched";
    /// `/tinkle.v1.Realtime/UnsubscribeBatched`: `UnsubscribeRequest` → `UnsubscribeResponse`.
    pub const UNSUBSCRIBE_BATCHED: &str = "/tinkle.v1.Realtime/UnsubscribeBatched";
}
