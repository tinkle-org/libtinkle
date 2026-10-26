# Only the test entry point is kept. SDK callees use AAR consumer rules.
-keep class com.tinkle.sdk.smoke.SmokeChecks { public static void run(); }
# The test APK also minifies AndroidX Test startup infrastructure.
-keep class androidx.test.platform.** { *; }
-keep class androidx.test.runner.** { *; }
-keep class androidx.test.monitor.** { *; }
# The runner needs Intrinsics overloads and Lambda stripped by app R8 otherwise.
# These sample-only keeps never ship in the SDK's consumer rules.
-keep class kotlin.** { *; }
