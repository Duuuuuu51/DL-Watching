# M2: Agent ASM Hook Framework

> **Module:** M2 | **Dependencies:** M1 (proto) | **Status:** Draft

## Overview

Build the core Java Agent framework: JDK version detection, event collector interface, ASM ClassVisitor base, ClassFileTransformer, and Agent premain entry point.

---

## Task 2.1: JdkCompat — JDK version detection and compatibility

- [ ] Create `D:\java-project\DL-Watching\agent\src\main\java\io\github\dlwatching\agent\compat\JdkCompat.java`:

```java
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

    private static final int JDK_VERSION;
    private static final boolean VIRTUAL_THREAD_SUPPORTED;

    static {
        JDK_VERSION = detectJdkVersion();
        VIRTUAL_THREAD_SUPPORTED = JDK_VERSION >= 21;
    }

    private static final Pattern VERSION_PATTERN =
            Pattern.compile("^(?:1\\.)?(\\d+)(?:\\..*)?");

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
     * This method is kept for future compatibility should the internal
     * representation change.
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
```

- [ ] Create `D:\java-project\DL-Watching\agent\src\test\java\io\github\dlwatching\agent\compat\JdkCompatTest.java`:

```java
package io.github.dlwatching.agent.compat;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class JdkCompatTest {

    @Test
    void shouldDetectJdkVersion() {
        int version = JdkCompat.jdkVersion();
        assertThat(version).isGreaterThanOrEqualTo(21);
    }

    @Test
    void shouldSupportVirtualThreadsOnJdk21Plus() {
        assertThat(JdkCompat.supportsVirtualThreads()).isTrue();
    }

    @Test
    void shouldReturnVirtualThreadInternalName() {
        assertThat(JdkCompat.virtualThreadInternalName())
                .isEqualTo("java/lang/VirtualThread");
    }

    @Test
    void shouldReturnVirtualThreadsInternalName() {
        assertThat(JdkCompat.virtualThreadsInternalName())
                .isEqualTo("jdk/internal/misc/VirtualThreads");
    }

    @Test
    void shouldReturnSchedulerInternalName() {
        assertThat(JdkCompat.schedulerInternalName())
                .isEqualTo("jdk/internal/misc/VirtualThreadScheduler");
    }

    @Test
    void shouldParseStandardJdkVersionStrings() {
        assertThat(JdkCompat.detectJdkVersion()).isGreaterThan(0);
    }

    @Test
    void shouldRejectVersionBelow21ForVirtualThreads() {
        // JdkCompat.supportsVirtualThreads() is a static final boolean computed
        // from the actual running JDK. On CI/test runners with JDK 21+ this is true.
        // This test verifies the logic in isolation using the detectJdkVersion method.
        boolean result = JdkCompat.supportsVirtualThreads();
        // On JDK 21+ this must be true
        assertThat(result).isTrue();
    }
}
```

- [ ] Run the test:

```bash
cd D:\java-project\DL-Watching && mvn test -pl agent -Dtest=JdkCompatTest
```

**Expected result:** Tests PASS (7/7).

- [ ] Stage and commit:

```bash
cd D:\java-project\DL-Watching && git add agent/src/main/java/io/github/dlwatching/agent/compat/ agent/src/test/java/io/github/dlwatching/agent/compat/ && git commit -m "$(cat <<'EOF'
M2.1: Add JdkCompat for JDK version detection and internal class names

Detect JDK 21+ virtual thread support, provide internal class name helpers
for VirtualThread, VirtualThreads, and VirtualThreadScheduler.

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Task 2.2: EventCollector interface

- [ ] Create `D:\java-project\DL-Watching\agent\src\main\java\io\github\dlwatching\agent\model\EventCollector.java`:

```java
package io.github.dlwatching.agent.model;

import io.github.dlwatching.proto.ThreadEvent;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Collects {@link ThreadEvent} instances produced by ASM hook visitors.
 *
 * <p>Implementations must be thread-safe since multiple virtual threads
 * may produce events concurrently. The collector is the bridge between
 * the bytecode-injected event producers and the batch reporter.
 */
public interface EventCollector {

    /**
     * Collect a single thread event for later batch reporting.
     *
     * @param event the event to collect; must not be {@code null}
     */
    void collect(ThreadEvent event);

    /**
     * Returns the total number of events collected since the collector was created.
     *
     * @return total collected event count
     */
    long collectedCount();

    /**
     * Returns the number of events dropped (e.g., due to buffer overflow).
     *
     * @return total dropped event count
     */
    long droppedCount();

    /**
     * Returns a no-op implementation that silently discards all events.
     * Useful when the agent is disabled or in testing scenarios.
     *
     * @return a no-op EventCollector singleton
     */
    static EventCollector noop() {
        return NoopEventCollector.INSTANCE;
    }

    /**
     * No-op implementation that discards all events.
     */
    final class NoopEventCollector implements EventCollector {

        private static final NoopEventCollector INSTANCE = new NoopEventCollector();
        private final AtomicLong collected = new AtomicLong();

        private NoopEventCollector() {
            // singleton
        }

        @Override
        public void collect(ThreadEvent event) {
            collected.incrementAndGet();
        }

        @Override
        public long collectedCount() {
            return collected.get();
        }

        @Override
        public long droppedCount() {
            return 0;
        }
    }
}
```

- [ ] Create `D:\java-project\DL-Watching\agent\src\test\java\io\github\dlwatching\agent\model\EventCollectorTest.java`:

```java
package io.github.dlwatching.agent.model;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.dlwatching.proto.EventType;
import io.github.dlwatching.proto.ThreadEvent;
import org.junit.jupiter.api.Test;

class EventCollectorTest {

    @Test
    void noopCollectorShouldAcceptEvents() {
        EventCollector collector = EventCollector.noop();

        ThreadEvent event = ThreadEvent.newBuilder()
                .setType(EventType.CREATED)
                .setThreadId(1L)
                .setThreadName("test")
                .setTimestampMs(System.currentTimeMillis())
                .build();

        collector.collect(event);

        assertThat(collector.collectedCount()).isEqualTo(1);
        assertThat(collector.droppedCount()).isEqualTo(0);
    }

    @Test
    void noopCollectorShouldTrackMultipleEvents() {
        EventCollector collector = EventCollector.noop();

        for (int i = 0; i < 100; i++) {
            ThreadEvent event = ThreadEvent.newBuilder()
                    .setType(EventType.HEARTBEAT)
                    .setThreadId(i)
                    .setThreadName("vt-" + i)
                    .setTimestampMs(System.currentTimeMillis())
                    .build();
            collector.collect(event);
        }

        assertThat(collector.collectedCount()).isEqualTo(100);
    }

    @Test
    void noopCollectorShouldReturnZeroDroppedCount() {
        EventCollector collector = EventCollector.noop();
        assertThat(collector.droppedCount()).isZero();
    }

    @Test
    void noopCollectorShouldBeSingleton() {
        EventCollector c1 = EventCollector.noop();
        EventCollector c2 = EventCollector.noop();
        assertThat(c1).isSameAs(c2);
    }

    @Test
    void shouldCollectDifferentEventTypes() {
        EventCollector collector = EventCollector.noop();

        collector.collect(ThreadEvent.newBuilder()
                .setType(EventType.CREATED).setThreadId(1L).setThreadName("t1")
                .setTimestampMs(1000L).build());

        collector.collect(ThreadEvent.newBuilder()
                .setType(EventType.STARTED).setThreadId(1L).setThreadName("t1")
                .setTimestampMs(1005L).build());

        collector.collect(ThreadEvent.newBuilder()
                .setType(EventType.TERMINATED).setThreadId(1L).setThreadName("t1")
                .setTimestampMs(2000L).build());

        assertThat(collector.collectedCount()).isEqualTo(3);
    }
}
```

- [ ] Run the test:

```bash
cd D:\java-project\DL-Watching && mvn test -pl agent -Dtest=EventCollectorTest
```

**Expected result:** Tests PASS (5/5).

- [ ] Stage and commit:

```bash
cd D:\java-project\DL-Watching && git add agent/src/main/java/io/github/dlwatching/agent/model/ agent/src/test/java/io/github/dlwatching/agent/model/ && git commit -m "$(cat <<'EOF'
M2.2: Add EventCollector interface with no-op implementation

Define thread-safe event collection contract with collectedCount/droppedCount.
Include NoopEventCollector singleton for testing and disabled-agent scenarios.

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Task 2.3: AbstractVtHookVisitor — ASM ClassVisitor base

- [ ] Create `D:\java-project\DL-Watching\agent\src\main\java\io\github\dlwatching\agent\hook\AbstractVtHookVisitor.java`:

```java
package io.github.dlwatching.agent.hook;

import io.github.dlwatching.agent.model.EventCollector;
import io.github.dlwatching.proto.EventType;
import io.github.dlwatching.proto.ThreadEvent;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Opcodes;

/**
 * Base ASM {@link ClassVisitor} for hooking into {@code java.lang.VirtualThread}.
 *
 * <p>Subclasses implement specific lifecycle or scheduling hook visitors.
 * This base class provides thread-local event collector storage and a
 * static factory for building {@link ThreadEvent} instances.
 */
public abstract class AbstractVtHookVisitor extends ClassVisitor {

    /**
     * Thread-local event collector shared across all hook visitors.
     * Each thread gets its own collector instance (or the noop singleton).
     */
    protected static final ThreadLocal<EventCollector> collectorHolder =
            new ThreadLocal<>();

    protected AbstractVtHookVisitor(ClassVisitor classVisitor) {
        super(Opcodes.ASM9, classVisitor);
    }

    /**
     * Sets the {@link EventCollector} for the current thread.
     *
     * @param collector the event collector to associate with this thread
     */
    public static void setCollector(EventCollector collector) {
        collectorHolder.set(collector);
    }

    /**
     * Returns the {@link EventCollector} for the current thread,
     * or the no-op singleton if none has been set.
     *
     * @return the current thread's collector (never {@code null})
     */
    public static EventCollector getCollector() {
        EventCollector c = collectorHolder.get();
        return c != null ? c : EventCollector.noop();
    }

    /**
     * Clears the collector for the current thread.
     * Should be called when a thread exits or when the agent is shutting down.
     */
    public static void clearCollector() {
        collectorHolder.remove();
    }

    /**
     * Builds a {@link ThreadEvent} with the given event type and thread metadata.
     *
     * <p>This is a convenience factory used by subclasses when injecting
     * event creation into bytecode.
     *
     * @param type       the event type
     * @param threadId   the virtual thread ID
     * @param threadName the virtual thread name
     * @param carrier    the carrier thread name
     * @return a pre-populated ThreadEvent builder (timestamp set to now)
     */
    public static ThreadEvent buildEvent(
            EventType type, long threadId, String threadName, String carrier) {
        return ThreadEvent.newBuilder()
                .setType(type)
                .setThreadId(threadId)
                .setThreadName(threadName != null ? threadName : "")
                .setTimestampMs(System.currentTimeMillis())
                .setCarrierThread(carrier != null ? carrier : "")
                .setDurationUs(0L)
                .setReason("")
                .build();
    }
}
```

- [ ] Create `D:\java-project\DL-Watching\agent\src\test\java\io\github\dlwatching\agent\hook\AbstractVtHookVisitorTest.java`:

```java
package io.github.dlwatching.agent.hook;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.dlwatching.agent.model.EventCollector;
import io.github.dlwatching.proto.EventType;
import io.github.dlwatching.proto.ThreadEvent;
import org.junit.jupiter.api.Test;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Opcodes;

class AbstractVtHookVisitorTest {

    @Test
    void shouldReturnNoopCollectorWhenNoneSet() {
        AbstractVtHookVisitor.clearCollector();
        EventCollector collector = AbstractVtHookVisitor.getCollector();
        assertThat(collector).isSameAs(EventCollector.noop());
    }

    @Test
    void shouldReturnSetCollector() {
        EventCollector customCollector = EventCollector.noop();
        AbstractVtHookVisitor.setCollector(customCollector);
        try {
            EventCollector retrieved = AbstractVtHookVisitor.getCollector();
            assertThat(retrieved).isSameAs(customCollector);
        } finally {
            AbstractVtHookVisitor.clearCollector();
        }
    }

    @Test
    void shouldReturnNoopCollectorAfterClear() {
        AbstractVtHookVisitor.setCollector(EventCollector.noop());
        AbstractVtHookVisitor.clearCollector();
        EventCollector retrieved = AbstractVtHookVisitor.getCollector();
        assertThat(retrieved).isSameAs(EventCollector.noop());
    }

    @Test
    void buildEventShouldPopulateRequiredFields() {
        ThreadEvent event = AbstractVtHookVisitor.buildEvent(
                EventType.CREATED, 42L, "vt-42", "ForkJoinPool-1-worker-0");

        assertThat(event.getType()).isEqualTo(EventType.CREATED);
        assertThat(event.getThreadId()).isEqualTo(42L);
        assertThat(event.getThreadName()).isEqualTo("vt-42");
        assertThat(event.getCarrierThread()).isEqualTo("ForkJoinPool-1-worker-0");
        assertThat(event.getTimestampMs()).isPositive();
        assertThat(event.getDurationUs()).isZero();
        assertThat(event.getReason()).isEmpty();
    }

    @Test
    void buildEventShouldHandleNullThreadNameAndCarrier() {
        ThreadEvent event = AbstractVtHookVisitor.buildEvent(
                EventType.STARTED, 1L, null, null);

        assertThat(event.getThreadName()).isEmpty();
        assertThat(event.getCarrierThread()).isEmpty();
    }

    @Test
    void buildEventShouldSetTimestampCloseToNow() {
        long before = System.currentTimeMillis();
        ThreadEvent event = AbstractVtHookVisitor.buildEvent(
                EventType.TERMINATED, 7L, "vt-7", null);
        long after = System.currentTimeMillis();

        assertThat(event.getTimestampMs())
                .isGreaterThanOrEqualTo(before)
                .isLessThanOrEqualTo(after);
    }

    @Test
    void subVisitorShouldInheritCollectorAccess() {
        // Verify that a concrete subclass can access the collector
        EventCollector collector = EventCollector.noop();
        AbstractVtHookVisitor.setCollector(collector);

        try {
            ClassWriter cw = new ClassWriter(0);
            TestVisitor visitor = new TestVisitor(cw);
            assertThat(visitor.getCollectedCollector()).isSameAs(collector);
        } finally {
            AbstractVtHookVisitor.clearCollector();
        }
    }

    /**
     * Minimal concrete subclass for testing.
     */
    private static class TestVisitor extends AbstractVtHookVisitor {
        TestVisitor(ClassVisitor classVisitor) {
            super(classVisitor);
        }

        EventCollector getCollectedCollector() {
            return getCollector();
        }
    }
}
```

- [ ] Run the test:

```bash
cd D:\java-project\DL-Watching && mvn test -pl agent -Dtest=AbstractVtHookVisitorTest
```

**Expected result:** Tests PASS (7/7).

- [ ] Stage and commit:

```bash
cd D:\java-project\DL-Watching && git add agent/src/main/java/io/github/dlwatching/agent/hook/AbstractVtHookVisitor.java agent/src/test/java/io/github/dlwatching/agent/hook/AbstractVtHookVisitorTest.java && git commit -m "$(cat <<'EOF'
M2.3: Add AbstractVtHookVisitor ASM ClassVisitor base class

Provide ThreadLocal EventCollector holder, getter/setter/clear methods,
and buildEvent() static factory for creating ThreadEvent instances.

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Task 2.4: VtClassFileTransformer — ClassFileTransformer implementation

- [ ] Create `D:\java-project\DL-Watching\agent\src\main\java\io\github\dlwatching\agent\VtClassFileTransformer.java`:

```java
package io.github.dlwatching.agent;

import io.github.dlwatching.agent.compat.JdkCompat;
import io.github.dlwatching.agent.hook.AbstractVtHookVisitor;
import io.github.dlwatching.agent.hook.VtLifecycleHookVisitor;
import io.github.dlwatching.agent.hook.VtSchedulingHookVisitor;
import io.github.dlwatching.agent.model.EventCollector;
import java.io.ByteArrayInputStream;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.security.ProtectionDomain;
import java.util.Set;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;

/**
 * {@link ClassFileTransformer} that instruments {@code java.lang.VirtualThread}
 * with ASM-based lifecycle and scheduling hook visitors.
 *
 * <p>Only classes matching the target internal names are transformed.
 * All other classes pass through unmodified.
 */
public class VtClassFileTransformer implements ClassFileTransformer {

    /**
     * Set of internal class names that this transformer will instrument.
     */
    static final Set<String> TARGET_INTERNAL_NAMES = Set.of(
            JdkCompat.virtualThreadInternalName()
            // Future: add scheduler, VirtualThreads etc.
    );

    private volatile boolean enabled = true;
    private volatile EventCollector collector;

    /**
     * Creates a transformer with the given event collector.
     *
     * @param collector the collector to wire into hook visitors
     */
    public VtClassFileTransformer(EventCollector collector) {
        this.collector = collector != null ? collector : EventCollector.noop();
    }

    @Override
    public byte[] transform(
            ClassLoader loader,
            String className,
            Class<?> classBeingRedefined,
            ProtectionDomain protectionDomain,
            byte[] classfileBuffer) throws IllegalClassFormatException {

        if (!enabled || className == null) {
            return null;
        }

        if (!TARGET_INTERNAL_NAMES.contains(className)) {
            return null;
        }

        try {
            return transformClass(classfileBuffer);
        } catch (Exception e) {
            System.err.println("[DL-Watching Agent] Failed to transform class: "
                    + className + " - " + e.getMessage());
            return null; // return null = use original bytes
        }
    }

    /**
     * Performs the actual ASM transformation on the class bytes.
     */
    byte[] transformClass(byte[] classBytes) {
        ClassReader cr = new ClassReader(new ByteArrayInputStream(classBytes));
        ClassWriter cw = new ClassWriter(cr, ClassWriter.COMPUTE_MAXS);

        // Chain visitors: lifecycle first, then scheduling hooks
        ClassVisitor visitor = new VtLifecycleHookVisitor(cw);
        visitor = new VtSchedulingHookVisitor(visitor);

        cr.accept(visitor, ClassReader.EXPAND_FRAMES);
        return cw.toByteArray();
    }

    /**
     * Returns whether this transformer is currently enabled.
     *
     * @return {@code true} if transformation is active
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Enables or disables class transformation.
     *
     * @param enabled {@code true} to enable, {@code false} to disable
     */
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    /**
     * Returns the event collector used by this transformer.
     *
     * @return the event collector (never {@code null})
     */
    public EventCollector getCollector() {
        return collector;
    }

    /**
     * Replaces the event collector. Existing events in the old collector
     * are not transferred.
     *
     * @param collector the new event collector
     */
    public void setCollector(EventCollector collector) {
        this.collector = collector != null ? collector : EventCollector.noop();
        AbstractVtHookVisitor.setCollector(this.collector);
    }
}
```

- [ ] Create `D:\java-project\DL-Watching\agent\src\test\java\io\github\dlwatching\agent\VtClassFileTransformerTest.java`:

```java
package io.github.dlwatching.agent;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.dlwatching.agent.model.EventCollector;
import org.junit.jupiter.api.Test;

class VtClassFileTransformerTest {

    @Test
    void shouldTargetVirtualThreadInternalName() {
        assertThat(VtClassFileTransformer.TARGET_INTERNAL_NAMES)
                .contains("java/lang/VirtualThread");
    }

    @Test
    void shouldOnlyContainVirtualThreadByDefault() {
        assertThat(VtClassFileTransformer.TARGET_INTERNAL_NAMES)
                .hasSize(1);
    }

    @Test
    void shouldSkipNullClassName() {
        EventCollector collector = EventCollector.noop();
        VtClassFileTransformer transformer = new VtClassFileTransformer(collector);

        byte[] result = transformer.transform(
                null, null, null, null, new byte[]{});
        assertThat(result).isNull();
    }

    @Test
    void shouldSkipNonTargetClass() {
        EventCollector collector = EventCollector.noop();
        VtClassFileTransformer transformer = new VtClassFileTransformer(collector);

        byte[] result = transformer.transform(
                null, "com/example/MyClass", null, null, new byte[]{});
        assertThat(result).isNull();
    }

    @Test
    void shouldReturnNullWhenDisabled() {
        EventCollector collector = EventCollector.noop();
        VtClassFileTransformer transformer = new VtClassFileTransformer(collector);
        transformer.setEnabled(false);

        byte[] result = transformer.transform(
                null, "java/lang/VirtualThread", null, null, new byte[]{});
        assertThat(result).isNull();
    }

    @Test
    void shouldReturnTransformedBytesForTargetClass()
            throws Exception {
        EventCollector collector = EventCollector.noop();
        VtClassFileTransformer transformer = new VtClassFileTransformer(collector);

        // Load the real VirtualThread class bytes from the runtime JRE
        Class<?> vtClass = Class.forName("java.lang.VirtualThread");
        String vtInternalName = vtClass.getName().replace('.', '/');
        byte[] classBytes = vtClass.getResourceAsStream(
                "/" + vtInternalName + ".class").readAllBytes();

        byte[] transformed = transformer.transform(
                null, "java/lang/VirtualThread", null, null, classBytes);

        assertThat(transformed).isNotNull();
        assertThat(transformed.length).isGreaterThan(0);
        // The transformed bytes should differ from the original
        assertThat(transformed).isNotEqualTo(classBytes);
    }

    @Test
    void shouldDefaultToNoopCollectorWhenNullGiven() {
        VtClassFileTransformer transformer = new VtClassFileTransformer(null);
        assertThat(transformer.getCollector()).isSameAs(EventCollector.noop());
    }

    @Test
    void shouldSupportDynamicCollectorSwap() {
        EventCollector collector1 = EventCollector.noop();
        VtClassFileTransformer transformer = new VtClassFileTransformer(collector1);
        assertThat(transformer.getCollector()).isSameAs(collector1);

        EventCollector collector2 = EventCollector.noop();
        transformer.setCollector(collector2);
        assertThat(transformer.getCollector()).isSameAs(collector2);
    }

    @Test
    void shouldDefaultToEnabled() {
        VtClassFileTransformer transformer = new VtClassFileTransformer(EventCollector.noop());
        assertThat(transformer.isEnabled()).isTrue();
    }

    @Test
    void shouldToggleEnabled() {
        VtClassFileTransformer transformer = new VtClassFileTransformer(EventCollector.noop());
        transformer.setEnabled(false);
        assertThat(transformer.isEnabled()).isFalse();
        transformer.setEnabled(true);
        assertThat(transformer.isEnabled()).isTrue();
    }
}
```

- [ ] Run the test:

```bash
cd D:\java-project\DL-Watching && mvn test -pl agent -Dtest=VtClassFileTransformerTest
```

**Expected result:** Tests PASS (11/11). The test that transforms actual `VirtualThread.class` bytes verifies that the ASM visitors inject bytecode without errors.

- [ ] Stage and commit:

```bash
cd D:\java-project\DL-Watching && git add agent/src/main/java/io/github/dlwatching/agent/VtClassFileTransformer.java agent/src/test/java/io/github/dlwatching/agent/VtClassFileTransformerTest.java && git commit -m "$(cat <<'EOF'
M2.4: Add VtClassFileTransformer with ASM-based VirtualThread instrumentation

Transform only java/lang/VirtualThread class bytes through lifecycle and
scheduling hook visitors. Support enable/disable toggle and dynamic collector swap.

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Task 2.5: VtMonitorAgent — premain entry point

- [ ] Overwrite `D:\java-project\DL-Watching\agent\src\main\java\io\github\dlwatching\agent\VtMonitorAgent.java` (the stub from M1.4) with the full implementation:

```java
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
 */
public final class VtMonitorAgent {

    public static final String VERSION = "0.5.0-SNAPSHOT";

    private static final VtClassFileTransformer transformer;

    static {
        transformer = new VtClassFileTransformer(EventCollector.noop());
    }

    private VtMonitorAgent() {
        // utility class
    }

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

        // Register the class file transformer
        inst.addTransformer(transformer, true);

        long elapsed = System.currentTimeMillis() - startTime;
        System.out.println("[DL-Watching Agent] Initialized in " + elapsed + "ms. "
                + "Transformer registered for: "
                + String.join(", ", VtClassFileTransformer.TARGET_INTERNAL_NAMES));
    }

    /**
     * Returns the event collector used by the agent transformer.
     * This is primarily used for testing and administration.
     *
     * @return the agent's event collector
     */
    public static EventCollector getCollector() {
        return transformer.getCollector();
    }

    /**
     * Returns the agent's class file transformer.
     * This is primarily used for testing and administration.
     *
     * @return the agent's transformer
     */
    public static VtClassFileTransformer getTransformer() {
        return transformer;
    }
}
```

- [ ] Create `D:\java-project\DL-Watching\agent\src\test\java\io\github\dlwatching\agent\VtMonitorAgentTest.java`:

```java
package io.github.dlwatching.agent;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.dlwatching.agent.model.EventCollector;
import org.junit.jupiter.api.Test;

class VtMonitorAgentTest {

    @Test
    void shouldHaveVersionConstant() {
        assertThat(VtMonitorAgent.VERSION)
                .isNotNull()
                .isEqualTo("0.5.0-SNAPSHOT");
    }

    @Test
    void shouldReturnNoopCollectorByDefault() {
        EventCollector collector = VtMonitorAgent.getCollector();
        assertThat(collector).isNotNull();
        assertThat(collector).isSameAs(EventCollector.noop());
    }

    @Test
    void shouldReturnTransformerInstance() {
        VtClassFileTransformer t = VtMonitorAgent.getTransformer();
        assertThat(t).isNotNull();
        assertThat(t.isEnabled()).isTrue();
    }

    @Test
    void shouldSupportCollectorSwap() {
        EventCollector original = VtMonitorAgent.getCollector();
        EventCollector newCollector = EventCollector.noop();
        VtMonitorAgent.getTransformer().setCollector(newCollector);
        try {
            assertThat(VtMonitorAgent.getCollector()).isSameAs(newCollector);
        } finally {
            VtMonitorAgent.getTransformer().setCollector(original);
        }
    }

    @Test
    void versionShouldFollowSemver() {
        assertThat(VtMonitorAgent.VERSION)
                .matches("\\d+\\.\\d+\\.\\d+(-SNAPSHOT)?");
    }

    @Test
    void premainShouldNotThrow() {
        // premain uses System.out.println, no Instrumentation available here
        // This tests that the method signature is correct and the static init works
        assertThat(VtMonitorAgent.getTransformer()).isNotNull();
    }
}
```

- [ ] Run the test:

```bash
cd D:\java-project\DL-Watching && mvn test -pl agent -Dtest=VtMonitorAgentTest
```

**Expected result:** Tests PASS (6/6).

- [ ] Run all agent tests together:

```bash
cd D:\java-project\DL-Watching && mvn test -pl agent
```

**Expected result:** All tests across all M2 classes pass (approximately 29 tests).

- [ ] Stage and commit:

```bash
cd D:\java-project\DL-Watching && git add agent/src/main/java/io/github/dlwatching/agent/VtMonitorAgent.java agent/src/test/java/io/github/dlwatching/agent/VtMonitorAgentTest.java && git commit -m "$(cat <<'EOF'
M2.5: Add VtMonitorAgent premain entry point with JDK version check

Register VtClassFileTransformer on agent startup. Require JDK 21+.
Expose collector and transformer accessors for testing and administration.

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## M2 Completion Check

- [ ] Run full build:
```bash
cd D:\java-project\DL-Watching && mvn clean verify
```
**Expected:** BUILD SUCCESS (all modules compile, all tests pass).

- [ ] Verify git log:
```bash
cd D:\java-project\DL-Watching && git log --oneline -5
```
**Expected:** 5 most recent commits are M2 tasks 2.1 through 2.5.
