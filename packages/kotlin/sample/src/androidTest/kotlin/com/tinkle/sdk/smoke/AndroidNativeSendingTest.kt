package com.tinkle.sdk.smoke

import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import org.junit.Test
import org.junit.runner.RunWith

/** Requires the installed matching native SDK. Compilation alone is not Android codec execution. */
@RunWith(AndroidJUnit4::class)
class AndroidNativeSendingTest {
    /** Normal host wiring encodes before offline upload, and retained retries need no encoder/source. */
    @Test fun normalVoiceAndAdoptionUseTheDurableManifest() {
        MediaSmokeChecks.normalVoiceAndAdoption(InstrumentationRegistry.getInstrumentation().targetContext)
    }
}
