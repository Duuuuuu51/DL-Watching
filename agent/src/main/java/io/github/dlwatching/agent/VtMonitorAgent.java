package io.github.dlwatching.agent;

import io.github.dlwatching.agent.compat.JdkCompat;
import io.github.dlwatching.agent.model.EventCollector;
import java.lang.instrument.Instrumentation;

/**
 * Java Agent premain entry point for DL-Watching Virtual Thread monitoring.
 *
 * <p>On startup, the agent:
 * <ol>
 *   <li>Checks JDK version compatibility (requires JDK 21+)</li>
 *   <li>Creates an {@link EventCollector} for buffering thread events</li>
 *   <li>Registers a {@link VtClassFileTransformer} to instrument
 *       {@code java.lang.VirtualThread}</li>
 *   <li>Wires the collector into {@code AbstractVtHookVisitor}'s thread-local</li>
 * </ol>
 *
 * @author Duuuuuu &lt;1617714380@qq.com&gt;
 */
public final class VtMonitorAgent {

    public static final String VERSION = "0.5.0-SNAPSHOT";

    private static final VtClassFileTransformer transformer;

    static {
        transformer = new VtClassFileTransformer(EventCollector.noop());
    }

    private VtMonitorAgent() {}

    /**
     * JVM agent premain entry point (Java 21+).
     * Called by the JVM before the application's {@code main} method.
     *
     * @param agentArgs agent command-line arguments
     * @param inst      instrumentation instance
     */
    public static void premain(String agentArgs, Instrumentation inst) {
        long startTime = System.currentTimeMillis();

        System.out.println("[DL-Watching Agent] v" + VERSION + " initializing...");

        if (!JdkCompat.supportsVirtualThreads()) {
            System.err.println("[DL-Watching Agent] WARNING: JDK " + JdkCompat.jdkVersion()
                    + " does not support virtual threads (JDK 21+ required). Agent disabled.");
            return;
        }

        System.out.println("[DL-Watching Agent] JDK " + JdkCompat.jdkVersion()
                + " detected. Virtual thread support: enabled.");

        inst.addTransformer(transformer, true);

        long elapsed = System.currentTimeMillis() - startTime;
        System.out.println("[DL-Watching Agent] Initialized in " + elapsed + "ms. "
                + "Transformer registered for: "
                + String.join(", ", VtClassFileTransformer.TARGET_INTERNAL_NAMES));
    }

    /**
     * Returns the event collector used by the agent transformer.
     */
    public static EventCollector getCollector() {
        return transformer.getCollector();
    }

    /**
     * Returns the agent's class file transformer.
     */
    public static VtClassFileTransformer getTransformer() {
        return transformer;
    }
}
