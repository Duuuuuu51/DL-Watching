package io.github.dlwatching.agent.hook;

import io.github.dlwatching.agent.model.EventCollector;
import io.github.dlwatching.proto.EventType;
import io.github.dlwatching.proto.ThreadEvent;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Opcodes;

public abstract class AbstractVtHookVisitor extends ClassVisitor {

    protected static final ThreadLocal<EventCollector> collectorHolder =
            new ThreadLocal<>();

    protected AbstractVtHookVisitor(ClassVisitor classVisitor) {
        super(Opcodes.ASM9, classVisitor);
    }

    public static void setCollector(EventCollector collector) {
        collectorHolder.set(collector);
    }

    public static EventCollector getCollector() {
        EventCollector c = collectorHolder.get();
        return c != null ? c : EventCollector.noop();
    }

    public static void clearCollector() {
        collectorHolder.remove();
    }

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
