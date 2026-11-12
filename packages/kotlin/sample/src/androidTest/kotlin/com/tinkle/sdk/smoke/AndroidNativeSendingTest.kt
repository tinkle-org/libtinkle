package com.tinkle.sdk.smoke

import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import com.tinkle.sdk.*
import java.io.File
import java.nio.ByteBuffer
import java.nio.ByteOrder
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith

/** Requires the installed matching native SDK. Compilation alone is not Android codec execution. */
@RunWith(AndroidJUnit4::class)
class AndroidNativeSendingTest {
    /** Normal host wiring encodes before offline upload, and retained retries need no encoder/source. */
    @Test fun normalVoiceAndAdoptionUseTheDurableManifest() = runBlocking {
        TinkleSdk.initialize()
        val directory = File(InstrumentationRegistry.getInstrumentation().targetContext.cacheDir, "sending-${System.nanoTime()}")
        check(directory.mkdir())
        val storage = SqliteStorage.openAsync(File(directory, "cache.sqlite").path)
        val trust = TrustSourceFfi.bootstrap(ServerTrustFfi(ByteArray(32) { 7 }, 7u, "native-sending-test"))
        val client = TinkleClient.newSqlite(ClientConfigFfi(ClientInfoFfi("fixture", "android", "1", "1", "1",
            ClientOptionsFfi("", "", "", "", "", "")), "127.0.0.1", 1u, null), trust, storage)
        try {
            client.setPrincipalUserId(42)
            client.configureAndroidMediaPreparer()
            val source = File(directory, "voice.pcm")
            val pcm = ByteBuffer.allocate(19200 * 2).order(ByteOrder.LITTLE_ENDIAN)
            repeat(19200) { pcm.putShort((kotlin.math.sin(it * 2.0 * kotlin.math.PI * 440 / 48000) * 16000).toInt().toShort()) }
            source.writeBytes(pcm.array())
            val kind = FfiMediaKind.Voice(48000u, 1u, 16u)
            suspend fun prepare(): FfiAttachmentManifest {
                try {
                    TransferControl().use { client.sendingPrepareAttachment("voice", source.path, kind, it) }
                    error("offline upload unexpectedly completed")
                } catch (e: FfiSendException.Rpc) { check(e.error is FfiException.Sync) }
                return checkNotNull(client.sendingAttachmentManifest("voice"))
            }
            val manifest = prepare()
            check(manifest.mimeType == "audio/mp4" && manifest.nativeFacts?.audioSamples == 19200uL)
            prepareNativeMedia(source.path, FfiNativeMediaKind.Voice(48000u, 1u, 16u), AndroidMediaPreparer()).use { artifact ->
                try {
                    TransferControl().use { client.sendingAdoptNativeAttachment("adopt", source.path, artifact, it) }
                    error("offline adoption unexpectedly completed")
                } catch (e: FfiSendException.Rpc) { check(e.error is FfiException.Sync) }
                check(checkNotNull(client.sendingAttachmentManifest("adopt")).outputSha256.contentEquals(artifact.outputSha256()))
            }
            client.clearNativeMediaPreparer()
            check(source.delete())
            val retry = prepare()
            check(retry.artifactPath == manifest.artifactPath && retry.outputSha256.contentEquals(manifest.outputSha256))
        } finally {
            client.shutdown()
            client.close()
            storage.clearPrincipal("user:42")
            storage.close()
            trust.close()
            directory.deleteRecursively()
        }
    }
}
