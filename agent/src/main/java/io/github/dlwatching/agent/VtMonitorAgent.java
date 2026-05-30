package io.github.dlwatching.agent;

import java.lang.instrument.Instrumentation;

/**
 * Entry point for the DL-Watching Java Agent.
 *
 * <p>Registered as {@code Premain-Class} in the shaded JAR manifest.
 * This stub will be fully implemented in M2.
 */
public final class VtMonitorAgent {

    public static final String VERSION = "0.5.0-SNAPSHOT";

    private VtMonitorAgent() {
        // utility class
    }

    /**
     * JVM agent premain entry point (Java 21+).
     *
     * @param agentArgs  agent command-line arguments
     * @param inst       instrumentation instance
     */
    public static void premain(String agentArgs, Instrumentation inst) {
        System.out.println("[DL-Watching Agent] v" + VERSION + " initializing...");
        // Full implementation will be added in M2.
    }
}
