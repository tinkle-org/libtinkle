package android.util

/** JVM-only shim for the fixture's sole Android API. Never included in the AAR/APK. */
object Base64 {
    const val NO_WRAP = 2
    fun encodeToString(bytes: ByteArray, flags: Int): String {
        require(flags == NO_WRAP)
        return java.util.Base64.getEncoder().encodeToString(bytes)
    }
}
