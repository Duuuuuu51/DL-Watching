package io.github.dlwatching.agent.compat;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class JdkCompat {

    private static final Pattern VERSION_PATTERN =
            Pattern.compile("^(?:1\\.)?(\\d+)(?:\\..*)?");

    private static final int JDK_VERSION;
    private static final boolean VIRTUAL_THREAD_SUPPORTED;

    static {
        JDK_VERSION = detectJdkVersion();
        VIRTUAL_THREAD_SUPPORTED = JDK_VERSION >= 21;
    }

    private JdkCompat() {}

    static int detectJdkVersion() {
        String version = System.getProperty("java.version", "0");
        Matcher m = VERSION_PATTERN.matcher(version);
        if (m.matches()) {
            return Integer.parseInt(m.group(1));
        }
        return 0;
    }

    public static int jdkVersion() {
        return JDK_VERSION;
    }

    public static boolean supportsVirtualThreads() {
        return VIRTUAL_THREAD_SUPPORTED;
    }

    public static String virtualThreadInternalName() {
        return "java/lang/VirtualThread";
    }

    public static String virtualThreadsInternalName() {
        return "jdk/internal/misc/VirtualThreads";
    }

    public static String schedulerInternalName() {
        return "jdk/internal/misc/VirtualThreadScheduler";
    }
}
