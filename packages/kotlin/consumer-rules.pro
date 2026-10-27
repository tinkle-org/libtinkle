# JNA calls native methods and reflects over Library methods, Callback invoke()
# methods, and Structure field names/constructors. These names are the ABI.
-keep class com.sun.jna.** { *; }
-keep interface com.sun.jna.** { *; }
# JNA's kept Native/Native$AWT desktop helpers reference AWT, absent on Android.
# https://github.com/java-native-access/jna/blob/5.18.1/www/FrequentlyAskedQuestions.md#jna-on-android
-dontwarn java.awt.Component
-dontwarn java.awt.GraphicsEnvironment
-dontwarn java.awt.HeadlessException
-dontwarn java.awt.Window
-keep class com.tinkle.sdk.** implements com.sun.jna.Library { *; }
-keep interface com.tinkle.sdk.** extends com.sun.jna.Library { *; }
-keep class com.tinkle.sdk.** implements com.sun.jna.Callback { *; }
-keep interface com.tinkle.sdk.** extends com.sun.jna.Callback { *; }
-keep class com.tinkle.sdk.** extends com.sun.jna.Structure { *; }
# UniFFI 0.32.2 uses JNA direct mapping: native method names are Rust exports.
-keepclasseswithmembers,includedescriptorclasses class com.tinkle.sdk.** {
    native <methods>;
}
-keepattributes InnerClasses,EnclosingMethod,Signature,*Annotation*
