package com.tinkle.sdk.smoke

import androidx.test.ext.junit.runners.AndroidJUnit4
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(AndroidJUnit4::class)
class NativeSmokeTest {
    @Test
    fun installedAarInitializesAndPersistsThroughRust() {
        try {
            SmokeChecks.run()
        } catch (error: Throwable) {
            // Gradle's summary truncates causes and suppressed fixture failures.
            // Keep the complete chain in device logcat before JUnit reports it.
            error.printStackTrace()
            throw error
        }
    }
}
