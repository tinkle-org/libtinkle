package com.tinkle.build

import groovy.json.JsonSlurper
import org.gradle.api.GradleException

import java.nio.file.Files
import java.security.MessageDigest
import java.util.zip.ZipFile

/** Build-time artifact reader. No native code, SDK source generation, or source-tree writes. */
class SdkArtifacts {
    public static final List<String> ABIS = ['arm64-v8a', 'armeabi-v7a', 'x86_64', 'x86']
    static final String BINDING = 'com/tinkle/sdk/tinkle_ffi.kt'
    static final Map<String, String> FILES = [
        ios: 'TinkleSDKFFI.xcframework.zip', swift_bindings: 'swift-bindings.zip',
        android: 'android-native-libraries.zip', kotlin_bindings: 'kotlin-bindings.zip'
    ]
    static final long MAX_ARCHIVE = 512L * 1024 * 1024
    static final long MAX_EXPANDED = 1024L * 1024 * 1024

    static void require(boolean condition, String message) {
        if (!condition) throw new GradleException(message)
    }

    static Map manifest(File file) {
        require(file.isFile(), 'No sdk-artifact.json release manifest. For bootstrap use -PsdkArtifactDir with both explicit SHA256 properties; see README.md.')
        require(file.length() <= 65536, 'Release manifest is too large')
        def data = new JsonSlurper().parse(file)
        require(data instanceof Map, 'Manifest must be an object')
        require(data.keySet() == ['schema_version', 'version', 'source_revision', 'source_repository', 'source_tag', 'artifacts'].toSet(), 'Invalid manifest fields')
        require(data.schema_version == 1, 'Unsupported SDK artifact schema')
        require(data.version instanceof String && data.version ==~ /(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?/, 'Invalid SDK version')
        require(data.source_revision instanceof String && data.source_revision ==~ /[0-9a-f]{40}/, 'Expected an exact Rust source revision')
        require(data.source_repository == 'tinkle-org/libtinkle', 'Unexpected SDK source repository')
        require(data.source_tag == "tinkle-ffi-v${data.version}".toString(), 'Source tag/version mismatch')
        require(data.artifacts instanceof Map && data.artifacts.keySet() == FILES.keySet(), 'Invalid release artifact set')
        FILES.each { key, name ->
            def entry = data.artifacts[key]
            require(entry instanceof Map && entry.keySet() == ['filename', 'url', 'sha256'].toSet(), "Invalid ${key} artifact fields")
            require(entry.filename == name, "Invalid ${key} filename")
            String canonical = "https://github.com/tinkle-org/libtinkle/releases/download/${data.source_tag}/${name}"
            require(entry.url == canonical, "${key} must use its canonical HTTPS GitHub versioned release URL")
            checksum(entry.sha256)
        }
        return data
    }

    static void checksum(Object value) {
        require(value instanceof String && value ==~ /[0-9a-f]{64}/, 'Expected a lowercase SHA256 checksum')
    }

    static String sha256(File file) {
        MessageDigest digest = MessageDigest.getInstance('SHA-256')
        file.withInputStream { stream ->
            byte[] buffer = new byte[65536]
            int count
            while ((count = stream.read(buffer)) != -1) digest.update(buffer, 0, count)
        }
        return digest.digest().encodeHex().toString()
    }

    static void verify(File file, String expected) {
        checksum(expected)
        require(file.isFile() && file.length() > 0 && file.length() <= MAX_ARCHIVE, "Missing or oversized SDK archive: ${file.name}")
        require(sha256(file) == expected, "SHA256 mismatch for ${file.name}")
    }

    static void download(String address, File output) {
        URI uri = new URI(address)
        for (int redirects = 0; redirects < 6; redirects++) {
            require(uri.scheme == 'https' && uri.userInfo == null && uri.port in [-1, 443] &&
                uri.host in ['github.com', 'release-assets.githubusercontent.com', 'objects.githubusercontent.com'], 'Unsafe SDK download/redirect URL')
            HttpURLConnection connection = (HttpURLConnection) uri.toURL().openConnection()
            connection.instanceFollowRedirects = false
            connection.connectTimeout = 30000
            connection.readTimeout = 60000
            connection.setRequestProperty('Accept', 'application/octet-stream')
            try {
                int code = connection.responseCode
                if (code in [301, 302, 303, 307, 308]) {
                    String location = connection.getHeaderField('Location')
                    require(location != null, 'Missing SDK redirect location')
                    uri = uri.resolve(location)
                    continue
                }
                require(code == 200, "SDK download returned HTTP ${code}")
                require(connection.contentLengthLong <= MAX_ARCHIVE, 'SDK download exceeds size limit')
                output.withOutputStream { target ->
                    connection.inputStream.withCloseable { source ->
                        copyBounded(source, target, MAX_ARCHIVE)
                    }
                }
                return
            } finally {
                connection.disconnect()
            }
        }
        throw new GradleException('Too many SDK download redirects')
    }

    static long copyBounded(InputStream source, OutputStream target, long limit) {
        byte[] buffer = new byte[65536]
        long total = 0
        int count
        while ((count = source.read(buffer)) != -1) {
            total += count
            require(total <= limit, 'SDK archive exceeds expanded/download size limit')
            target.write(buffer, 0, count)
        }
        return total
    }

    static Set<String> extract(File archive, File destination, Set<String> expected) {
        Set<String> found = [] as Set
        Set<String> entries = [] as Set
        long expanded = 0
        new ZipFile(archive).withCloseable { zip ->
            def iterator = zip.entries()
            while (iterator.hasMoreElements()) {
                def entry = iterator.nextElement()
                String name = entry.name
                require(entries.size() < 64 && entries.add(name), 'Duplicate or excessive ZIP entries')
                require(name && !name.startsWith('/') && !name.contains('\\') &&
                    !name.contains(':') && !name.split('/').any { it in ['', '.', '..'] }, "Unsafe ZIP entry: ${name}")
                if (entry.directory) {
                    require(expected.any { it.startsWith(name) }, "Unexpected ZIP directory: ${name}")
                    continue
                }
                require(name in expected && found.add(name), "Unexpected ZIP file: ${name}")
                require(entry.size >= 0 && entry.size <= MAX_EXPANDED, "Oversized ZIP entry: ${name}")
                File target = new File(destination, name)
                require(target.canonicalPath.startsWith(destination.canonicalPath + File.separator), 'ZIP path escaped destination')
                require(!Files.isSymbolicLink(target.toPath()), 'Refusing symlink extraction target')
                target.parentFile.mkdirs()
                target.withOutputStream { output ->
                    zip.getInputStream(entry).withCloseable { input ->
                        expanded += copyBounded(input, output, MAX_EXPANDED - expanded)
                    }
                }
            }
        }
        require(found == expected, 'SDK ZIP is missing required files/ABIs')
        return found
    }
}
