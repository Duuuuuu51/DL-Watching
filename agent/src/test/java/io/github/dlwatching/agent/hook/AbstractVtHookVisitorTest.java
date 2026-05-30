package io.github.dlwatching.agent.hook;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.dlwatching.agent.model.EventCollector;
import io.github.dlwatching.proto.EventType;
import io.github.dlwatching.proto.ThreadEvent;
import org.junit.jupiter.api.Test;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;

class AbstractVtHookVisitorTest {

    @Test
    void shouldReturnNoopCollectorWhenNoneSet() {
        AbstractVtHookVisitor.clearCollector();
        EventCollector collector = AbstractVtHookVisitor.getCollector();
        assertThat(collector).isNotNull();
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
        assertThat(retrieved).isNotNull();
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

    private static class TestVisitor extends AbstractVtHookVisitor {
        TestVisitor(ClassVisitor classVisitor) {
            super(classVisitor);
        }
        EventCollector getCollectedCollector() {
            return getCollector();
        }
    }
}
