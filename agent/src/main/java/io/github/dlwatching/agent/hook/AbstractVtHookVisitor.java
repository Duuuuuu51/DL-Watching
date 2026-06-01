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
