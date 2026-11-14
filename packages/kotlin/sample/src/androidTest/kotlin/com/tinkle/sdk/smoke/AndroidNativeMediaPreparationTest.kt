package com.tinkle.sdk.smoke

import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import org.junit.Test
import org.junit.runner.RunWith

/** Requires the installed native SDK and real OS codecs; these are not JVM codec simulations. */
@RunWith(AndroidJUnit4::class)
class AndroidNativeMediaPreparationTest {
    @Test fun pcmVoiceAndImportedAudioAreRealAacOnlyMp4() {
        MediaSmokeChecks.pcmVoiceAndImportedAudio(InstrumentationRegistry.getInstrumentation().targetContext)
    }

    @Test fun alphaPngAndOrientedJpegBecomeVerifiedLosslessImages() {
        MediaSmokeChecks.alphaPngAndOrientedJpeg(InstrumentationRegistry.getInstrumentation().targetContext)
    }
}
