package com.tinkle.sdk.smoke

import androidx.test.ext.junit.runners.AndroidJUnit4
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(AndroidJUnit4::class)
class NativeSmokeTest {
    @Test
    fun installedAarInitializesAndPersistsThroughRust() {
        SmokeChecks.run()
    }
}
