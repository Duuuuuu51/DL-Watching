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
     * Resets the singleton no-op collector's internal counters.
     * Package-private; intended for test use only.
     */
    static void resetNoop() {
        NoopEventCollector.INSTANCE.collected.set(0);
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
