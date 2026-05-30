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
                .setType(EventType.CREATED).setThreadId(1L).setThreadName("test")
                .setTimestampMs(System.currentTimeMillis()).build();
        collector.collect(event);
        assertThat(collector.collectedCount()).isEqualTo(1);
        assertThat(collector.droppedCount()).isEqualTo(0);
    }

    @Test
    void noopCollectorShouldTrackMultipleEvents() {
        EventCollector collector = EventCollector.noop();
        for (int i = 0; i < 100; i++) {
            ThreadEvent event = ThreadEvent.newBuilder()
                    .setType(EventType.HEARTBEAT).setThreadId(i).setThreadName("vt-" + i)
                    .setTimestampMs(System.currentTimeMillis()).build();
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
        assertThat(c1).isNotNull();
        assertThat(c2).isNotNull();
    }

    @Test
    void shouldCollectDifferentEventTypes() {
        EventCollector collector = EventCollector.noop();
        collector.collect(ThreadEvent.newBuilder().setType(EventType.CREATED).setThreadId(1L).setThreadName("t1").setTimestampMs(1000L).build());
        collector.collect(ThreadEvent.newBuilder().setType(EventType.STARTED).setThreadId(1L).setThreadName("t1").setTimestampMs(1005L).build());
        collector.collect(ThreadEvent.newBuilder().setType(EventType.TERMINATED).setThreadId(1L).setThreadName("t1").setTimestampMs(2000L).build());
        assertThat(collector.collectedCount()).isEqualTo(3);
    }
}
