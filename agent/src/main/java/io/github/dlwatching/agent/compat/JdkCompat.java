package io.github.dlwatching.agent.compat;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Determines JDK version and virtual thread support at runtime.
 *
 * <p>Java 21+ is required for virtual threads. This class detects the exact
 * JDK version and provides adapter methods for VM-internal class names
 * that differ across JDK releases.
 */
public final class JdkCompat {

    private static final Pattern VERSION_PATTERN =
            Pattern.compile("^(?:1\\.)?(\\d+)(?:\\..*)?");

    private static final int JDK_VERSION;
    private static final boolean VIRTUAL_THREAD_SUPPORTED;

    static {
        JDK_VERSION = detectJdkVersion();
        VIRTUAL_THREAD_SUPPORTED = JDK_VERSION >= 21;
    }

    private JdkCompat() {
        // utility class
    }

    /**
     * Detects the major JDK version from {@code java.version} system property.
     *
     * @return the major version (e.g., 21 for "21.0.2", 8 for "1.8.0_202")
     */
    static int detectJdkVersion() {
        String version = System.getProperty("java.version", "0");
        Matcher m = VERSION_PATTERN.matcher(version);
        if (m.matches()) {
            return Integer.parseInt(m.group(1));
        }
        return 0;
    }

    /**
     * Returns the current JDK major version.
     *
     * @return JDK major version number (e.g., 21)
     */
    public static int jdkVersion() {
        return JDK_VERSION;
    }

    /**
     * Returns whether the current JDK supports virtual threads (JDK 21+).
     *
     * @return {@code true} if JDK version >= 21
     */
    public static boolean supportsVirtualThreads() {
        return VIRTUAL_THREAD_SUPPORTED;
    }

    /**
     * Returns the internal JVM class name for {@link java.lang.VirtualThread}.
     *
     * <p>In JDK 21+, the internal name is {@code java/lang/VirtualThread}.
     *
     * @return JVM internal class name (e.g., {@code "java/lang/VirtualThread"})
     */
    public static String virtualThreadInternalName() {
        return "java/lang/VirtualThread";
    }

    /**
     * Returns the internal JVM class name for
     * {@code jdk.internal.misc.VirtualThreads}.
     *
     * <p>This is a JDK internal support class used for park/unpark operations.
     *
     * @return JVM internal class name
     */
    public static String virtualThreadsInternalName() {
        return "jdk/internal/misc/VirtualThreads";
    }

    /**
     * Returns the internal JVM class name for the virtual thread scheduler.
     *
     * @return JVM internal class name
     */
    public static String schedulerInternalName() {
        return "jdk/internal/misc/VirtualThreadScheduler";
    }
}
