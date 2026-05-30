package io.github.dlwatching.agent;

import java.lang.instrument.Instrumentation;

public final class VtMonitorAgent {
    public static final String VERSION = "0.5.0-SNAPSHOT";
    private VtMonitorAgent() {}

    public static void premain(String agentArgs, Instrumentation inst) {
        System.out.println("[DL-Watching Agent] v" + VERSION + " initializing...");
    }
}
