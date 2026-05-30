package io.github.dlwatching.agent.model;

import io.github.dlwatching.proto.ThreadEvent;
import java.util.concurrent.atomic.AtomicLong;

public interface EventCollector {

    void collect(ThreadEvent event);

    long collectedCount();

    long droppedCount();

    static EventCollector noop() {
        return new NoopEventCollector();
    }

    final class NoopEventCollector implements EventCollector {

        private final AtomicLong collected = new AtomicLong();

        NoopEventCollector() {}

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
