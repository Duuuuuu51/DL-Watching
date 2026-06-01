package io.github.dlwatching.agent.cache;

import io.github.dlwatching.proto.EventBatch;
import io.github.dlwatching.proto.ThreadEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Time+count dual-trigger batch aggregator that drains a {@link RingBuffer}
 * into {@link EventBatch} messages and delivers them via {@link BatchListener}.
 *
 * <p>A flush is triggered by either:
 * <ul>
 *   <li>The count trigger — when {@link #signalEvent()} detects the buffer
 *       has reached {@code maxBatchSize}</li>
 *   <li>The timer trigger — a scheduled task fires every {@code flushIntervalMs}</li>
 * </ul>
 *
 * @author Duuuuuu &lt;1617714380@qq.com&gt; @since 2026-06-01
 */
public class BatchAggregator {

    private static final int DEFAULT_MAX_BATCH_SIZE = 500;
    private static final long DEFAULT_FLUSH_INTERVAL_MS = 3000;

    private final RingBuffer<ThreadEvent> buffer;
    private final int maxBatchSize;
    private final long flushIntervalMs;
    private final BatchListener listener;
    private final AtomicLong batchSeq = new AtomicLong(0);
    private final String appId;
    private final String instanceId;
    private final ReentrantLock flushLock = new ReentrantLock();

    private ScheduledExecutorService scheduler;
    private volatile boolean running;

    public BatchAggregator(RingBuffer<ThreadEvent> buffer, BatchListener listener,
                           String appId, String instanceId) {
        this(buffer, DEFAULT_MAX_BATCH_SIZE, DEFAULT_FLUSH_INTERVAL_MS, listener, appId, instanceId);
    }

    public BatchAggregator(RingBuffer<ThreadEvent> buffer, int maxBatchSize,
                           long flushIntervalMs, BatchListener listener,
                           String appId, String instanceId) {
        if (buffer == null) {
            throw new IllegalArgumentException("Buffer must not be null");
        }
        if (maxBatchSize <= 0) {
            throw new IllegalArgumentException("MaxBatchSize must be positive: " + maxBatchSize);
        }
        if (flushIntervalMs <= 0) {
            throw new IllegalArgumentException("FlushIntervalMs must be positive: " + flushIntervalMs);
        }
        if (listener == null) {
            throw new IllegalArgumentException("Listener must not be null");
        }
        this.buffer = buffer;
        this.maxBatchSize = maxBatchSize;
        this.flushIntervalMs = flushIntervalMs;
        this.listener = listener;
        this.appId = appId;
        this.instanceId = instanceId;
    }

    public void start() {
        if (running) {
            return;
        }
        running = true;
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "batch-aggregator");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(this::flush, flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        running = false;
        if (scheduler != null) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(2, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        flush();
    }

    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    public long getFlushIntervalMs() {
        return flushIntervalMs;
    }

    public boolean isRunning() {
        return running;
    }

    /**
     * Signal that a new event has been added to the buffer.
     * If the buffer has reached maxBatchSize, a flush is triggered immediately.
     */
    public void signalEvent() {
        if (running && buffer.size() >= maxBatchSize) {
            flush();
        }
    }

    /**
     * Drains up to maxBatchSize events from the ring buffer and sends them as an EventBatch.
     */
    void flush() {
        if (!flushLock.tryLock()) {
            return;
        }
        try {
            if (!running && buffer.size() == 0) {
                return;
            }
            List<ThreadEvent> events = new ArrayList<>();
            ThreadEvent event;
            while ((event = buffer.poll()) != null && events.size() < maxBatchSize) {
                events.add(event);
            }
            if (events.isEmpty()) {
                return;
            }
            long seq = batchSeq.getAndIncrement();
            long now = System.currentTimeMillis();
            EventBatch batch = EventBatch.newBuilder()
                    .setAppId(appId)
                    .setInstanceId(instanceId)
                    .setBatchSeq(seq)
                    .setTimestampMs(now)
                    .addAllEvents(events)
                    .build();
            listener.onBatch(batch);
        } finally {
            flushLock.unlock();
        }
    }
}
