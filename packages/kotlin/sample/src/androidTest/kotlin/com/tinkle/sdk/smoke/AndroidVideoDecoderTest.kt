package com.tinkle.sdk.smoke

import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import org.junit.Test
import org.junit.runner.RunWith

/** Runs only in the installed final-AAR consumer, never as a host JVM codec simulation. */
@RunWith(AndroidJUnit4::class)
class AndroidVideoDecoderTest {
    @Test fun decodesActualFramesAndDerivesBoundedSrgb() = VideoSmokeChecks.decodePlatform(context())
    @Test fun displayRotationIsAppliedToActualDecodedPixels() = VideoSmokeChecks.decodeDisplayRotation(context())
    @Test fun budgetsAndMalformedInputReleaseNativeResources() = VideoSmokeChecks.rejectsInvalidAndOverBudget(context())
    @Test fun coroutineCancellationWaitsForActualCodecCleanup() = VideoSmokeChecks.cancellationJoinsWorkerBeforeClosingLease(context())
    @Test fun nativeSdkCallbacksAndCancelledSourceLease() = VideoSmokeChecks.nativeCallbackAndLease(context())

    private fun context() = InstrumentationRegistry.getInstrumentation().targetContext
}
