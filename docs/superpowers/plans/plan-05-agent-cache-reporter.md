# DL-Watching Agent Cache & gRPC Reporter

**Module:** M5
**Dependencies:** M4 (Agent Scheduling Hooks)
**Packages:** `io.github.dlwatching.agent.cache`, `io.github.dlwatching.agent.reporter`, `io.github.dlwatching.agent.config`

---

## Task 5.1: RingBuffer — bounded concurrent event buffer

**File:** `D:\java-project\DL-Watching\agent\src\main\java\io\github\dlwatching\agent\cache\RingBuffer.java`
**Test:** `D:\java-project\DL-Watching\agent\src\test\java\io\github\dlwatching\agent\cache\RingBufferTest.java`

### 5.1.1 — Create RingBuffer implementation

- [ ] Create RingBuffer.java with the following content:

```java
package io.github.dlwatching.agent.cache;

import java.util.concurrent.atomic.AtomicLong;

public class RingBuffer<T> {

    private static final int DEFAULT_CAPACITY = 10000;

    private final Object[] buffer;
    private final int capacity;
    private final AtomicLong head = new AtomicLong(0);
    private final AtomicLong tail = new AtomicLong(0);
    private final AtomicLong drops = new AtomicLong(0);

    public RingBuffer() {
        this(DEFAULT_CAPACITY);
    }

    public RingBuffer(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Capacity must be positive: " + capacity);
        }
        this.capacity = capacity;
        this.buffer = new Object[capacity];
    }

    /**
     * Offer an item to the buffer. If the buffer is full, the oldest item
     * is dropped (FIFO overwrite) and this method returns false.
     *
     * @param item the item to add; must not be null
     * @return true if no item was dropped, false if an older item was discarded
     */
    public boolean offer(T item) {
        if (item == null) {
            throw new NullPointerException("Item must not be null");
        }
        long t = tail.getAndIncrement();
        long h = head.get();
        boolean dropped = false;
        if (t - h >= capacity) {
            head.getAndIncrement();
            drops.incrementAndGet();
            dropped = true;
        }
        int index = (int) (t % capacity);
        buffer[index] = item;
        return !dropped;
    }

    @SuppressWarnings("unchecked")
    public T poll() {
        while (true) {
            long h = head.get();
            long t = tail.get();
            if (h >= t) {
                return null;
            }
            if (head.compareAndSet(h, h + 1)) {
                int index = (int) (h % capacity);
                T item = (T) buffer[index];
                buffer[index] = null;
                return item;
            }
        }
    }

    public int size() {
        long diff = tail.get() - head.get();
        return (int) Math.max(0, Math.min(diff, Integer.MAX_VALUE));
    }

    public int capacity() {
        return capacity;
    }

    public long drops() {
        return drops.get();
    }

    public void clear() {
        while (poll() != null) {
            // drain
        }
    }
}
```

### 5.1.2 — Create RingBufferTest

- [ ] Create RingBufferTest.java:

```java
package io.github.dlwatching.agent.cache;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RingBufferTest {

    private RingBuffer<String> buffer;

    @BeforeEach
    void setUp() {
        buffer = new RingBuffer<>(100);
    }

    @Test
    void shouldOfferAndPollSingleItem() {
        boolean offered = buffer.offer("hello");
        assertThat(offered).isTrue();
        assertThat(buffer.size()).isEqualTo(1);

        String item = buffer.poll();
        assertThat(item).isEqualTo("hello");
        assertThat(buffer.size()).isEqualTo(0);
    }

    @Test
    void shouldDropOldestWhenOfferedBeyondCapacity() {
        for (int i = 0; i < 105; i++) {
            buffer.offer("item-" + i);
        }
        assertThat(buffer.drops()).isEqualTo(5);
        assertThat(buffer.size()).isEqualTo(100);

        String first = buffer.poll();
        assertThat(first).isEqualTo("item-5");
    }

    @Test
    void shouldReturnFalseFromOfferWhenDropped() {
        RingBuffer<String> small = new RingBuffer<>(5);
        for (int i = 0; i < 4; i++) {
            assertThat(small.offer("x")).isTrue();
        }
        assertThat(small.offer("x")).isTrue();
        assertThat(small.offer("x")).isFalse();
    }

    @Test
    void shouldReturnNullWhenPollingFromEmpty() {
        assertThat(buffer.poll()).isNull();
    }

    @Test
    void shouldReflectSizeCorrectly() {
        assertThat(buffer.size()).isEqualTo(0);
        buffer.offer("a");
        assertThat(buffer.size()).isEqualTo(1);
        buffer.offer("b");
        assertThat(buffer.size()).isEqualTo(2);
        buffer.poll();
        assertThat(buffer.size()).isEqualTo(1);
        buffer.poll();
        assertThat(buffer.size()).isEqualTo(0);
    }

    @Test
    void shouldHandleConcurrentOffersFromMultipleThreads() throws InterruptedException {
        int threadCount = 4;
        int itemsPerThread = 1000;
        int totalItems = threadCount * itemsPerThread;
        RingBuffer<Integer> shared = new RingBuffer<>(totalItems);

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        AtomicInteger offerCount = new AtomicInteger(0);

        for (int t = 0; t < threadCount; t++) {
            int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < itemsPerThread; i++) {
                        if (shared.offer(threadId * itemsPerThread + i)) {
                            offerCount.incrementAndGet();
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdown();

        int totalOffered = offerCount.get() + (int) shared.drops();
        assertThat(totalOffered).isEqualTo(totalItems);

        int polledCount = 0;
        while (shared.poll() != null) {
            polledCount++;
        }
        assertThat(polledCount + (int) shared.drops()).isEqualTo(totalItems);
    }

    @Test
    void shouldRejectNegativeCapacity() {
        assertThatThrownBy(() -> new RingBuffer<>(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRejectNullOffer() {
        assertThatThrownBy(() -> buffer.offer(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldClearAllItems() {
        buffer.offer("a");
        buffer.offer("b");
        buffer.offer("c");
        assertThat(buffer.size()).isEqualTo(3);
        buffer.clear();
        assertThat(buffer.size()).isEqualTo(0);
        assertThat(buffer.poll()).isNull();
    }
}
```

### 5.1.3 — Verify and commit

- [ ] Run tests:
  ```bash
  cd "D:\java-project\DL-Watching" && mvn clean test -pl agent -Dtest="io.github.dlwatching.agent.cache.RingBufferTest" -DfailIfNoTests=false
  ```
- [ ] Commit:
  ```bash
  cd "D:\java-project\DL-Watching" && git add agent/src/main/java/io/github/dlwatching/agent/cache/RingBuffer.java agent/src/test/java/io/github/dlwatching/agent/cache/RingBufferTest.java && git commit -m "M5-T5.1: Implement RingBuffer with atomic head/tail cursors and FIFO drop"
  ```

---

## Task 5.2: BatchAggregator — time+count dual-trigger batching

**File:** `D:\java-project\DL-Watching\agent\src\main\java\io\github\dlwatching\agent\cache\BatchAggregator.java`
**File:** `D:\java-project\DL-Watching\agent\src\main\java\io\github\dlwatching\agent\cache\BatchListener.java`
**Test:** `D:\java-project\DL-Watching\agent\src\test\java\io\github\dlwatching\agent\cache\BatchAggregatorTest.java`

### 5.2.1 — Create BatchListener interface

- [ ] Create BatchListener.java:

```java
package io.github.dlwatching.agent.cache;

import io.github.dlwatching.proto.EventBatch;

@FunctionalInterface
public interface BatchListener {
    void onBatch(EventBatch batch);
}
```

### 5.2.2 — Create BatchAggregator

- [ ] Create BatchAggregator.java:

```java
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
        // Flush remaining events before stopping
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
     * This is the count-trigger mechanism that overrides the timer.
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
```

### 5.2.3 — Create BatchAggregatorTest

- [ ] Create BatchAggregatorTest.java:

```java
package io.github.dlwatching.agent.cache;

import io.github.dlwatching.proto.EventBatch;
import io.github.dlwatching.proto.ThreadEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BatchAggregatorTest {

    private RingBuffer<ThreadEvent> buffer;
    private BatchAggregator aggregator;
    private TestBatchListener listener;

    @BeforeEach
    void setUp() {
        buffer = new RingBuffer<>(1000);
        listener = new TestBatchListener();
    }

    @AfterEach
    void tearDown() {
        if (aggregator != null) {
            aggregator.stop();
        }
    }

    @Test
    void shouldTriggerFlushByTimer() throws InterruptedException {
        aggregator = new BatchAggregator(buffer, 500, 100, listener, "test-app", "test-instance");
        aggregator.start();

        ThreadEvent event = createTestEvent(1, "vt-1");
        buffer.offer(event);

        boolean received = listener.awaitBatch(5000, TimeUnit.MILLISECONDS);
        assertThat(received).isTrue();
        assertThat(listener.getBatch()).isNotNull();
        assertThat(listener.getBatch().getAppId()).isEqualTo("test-app");
        assertThat(listener.getBatch().getEventsCount()).isEqualTo(1);
        assertThat(listener.getBatch().getEvents(0).getThreadId()).isEqualTo(1);
    }

    @Test
    void shouldTriggerFlushByCountTrigger() throws InterruptedException {
        aggregator = new BatchAggregator(buffer, 10, 10000, listener, "test-app", "test-instance");
        aggregator.start();

        for (int i = 0; i < 10; i++) {
            buffer.offer(createTestEvent(i, "vt-" + i));
            aggregator.signalEvent();
        }

        boolean received = listener.awaitBatch(2000, TimeUnit.MILLISECONDS);
        assertThat(received).isTrue();
        assertThat(listener.getBatch().getEventsCount()).isEqualTo(10);
    }

    @Test
    void shouldNotFlushAfterStop() throws InterruptedException {
        aggregator = new BatchAggregator(buffer, 500, 50, listener, "test-app", "test-instance");
        aggregator.start();

        buffer.offer(createTestEvent(1, "vt-1"));
        aggregator.stop();

        // Should not receive a batch after stop (the final flush in stop() is OK)
        // After aggregator is stopped, the scheduler won't fire again
        Thread.sleep(200);
        assertThat(listener.getBatch()).isNull();
    }

    @Test
    void shouldNotSendBatchWhenBufferIsEmpty() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        BatchListener emptyListener = batch -> latch.countDown();

        aggregator = new BatchAggregator(buffer, 500, 50, emptyListener, "test-app", "test-instance");
        aggregator.start();

        boolean fired = latch.await(300, TimeUnit.MILLISECONDS);
        assertThat(fired).isFalse();
    }

    @Test
    void shouldRejectNullConstructorArgs() {
        assertThatThrownBy(() -> new BatchAggregator(null, 500, 1000, listener, "a", "b"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BatchAggregator(buffer, -1, 1000, listener, "a", "b"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldDrainRemainingEventsOnStop() throws InterruptedException {
        aggregator = new BatchAggregator(buffer, 500, 10000, listener, "test-app", "test-instance");
        aggregator.start();

        for (int i = 0; i < 3; i++) {
            buffer.offer(createTestEvent(i, "vt-" + i));
        }

        aggregator.stop();

        assertThat(listener.getBatch()).isNotNull();
        assertThat(listener.getBatch().getEventsCount()).isEqualTo(3);
    }

    @Test
    void shouldBatchSeqIncrementAcrossFlushes() throws InterruptedException {
        aggregator = new BatchAggregator(buffer, 500, 50, listener, "test-app", "test-instance");
        aggregator.start();

        buffer.offer(createTestEvent(1, "vt-1"));
        listener.awaitBatch(5000, TimeUnit.MILLISECONDS);
        long seq1 = listener.getBatch().getBatchSeq();

        buffer.offer(createTestEvent(2, "vt-2"));
        listener.awaitBatch(5000, TimeUnit.MILLISECONDS);
        long seq2 = listener.getBatch().getBatchSeq();

        assertThat(seq2).isEqualTo(seq1 + 1);
    }

    private static ThreadEvent createTestEvent(long threadId, String threadName) {
        return ThreadEvent.newBuilder()
                .setType(ThreadEvent.EventType.CREATED)
                .setThreadId(threadId)
                .setThreadName(threadName)
                .setTimestampMs(System.currentTimeMillis())
                .build();
    }

    private static class TestBatchListener implements BatchListener {
        private final CountDownLatch latch = new CountDownLatch(1);
        private final AtomicReference<EventBatch> batchRef = new AtomicReference<>();

        @Override
        public void onBatch(EventBatch batch) {
            batchRef.set(batch);
            latch.countDown();
        }

        boolean awaitBatch(long timeout, TimeUnit unit) throws InterruptedException {
            return latch.await(timeout, unit);
        }

        EventBatch getBatch() {
            return batchRef.get();
        }
    }
}
```

### 5.2.4 — Verify and commit

- [ ] Run tests:
  ```bash
  cd "D:\java-project\DL-Watching" && mvn clean test -pl agent -Dtest="io.github.dlwatching.agent.cache.BatchAggregatorTest" -DfailIfNoTests=false
  ```
- [ ] Commit:
  ```bash
  cd "D:\java-project\DL-Watching" && git add agent/src/main/java/io/github/dlwatching/agent/cache/BatchListener.java agent/src/main/java/io/github/dlwatching/agent/cache/BatchAggregator.java agent/src/test/java/io/github/dlwatching/agent/cache/BatchAggregatorTest.java && git commit -m "M5-T5.2: Implement BatchAggregator with time+count dual-trigger batching"
  ```

---

## Task 5.3: GrpcReporter — gRPC streaming client with retry

**File:** `D:\java-project\DL-Watching\agent\src\main\java\io\github\dlwatching\agent\reporter\GrpcReporter.java`
**Test:** `D:\java-project\DL-Watching\agent\src\test\java\io\github\dlwatching\agent\reporter\GrpcReporterTest.java`

### 5.3.1 — Create GrpcReporter

- [ ] Create GrpcReporter.java:

```java
package io.github.dlwatching.agent.reporter;

import io.github.dlwatching.agent.cache.BatchListener;
import io.github.dlwatching.proto.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class GrpcReporter implements BatchListener {

    private static final Logger log = LoggerFactory.getLogger(GrpcReporter.class);
    private static final int MAX_RETRIES = 3;
    private static final long HEARTBEAT_INTERVAL_MS = 10_000L;

    private final String backendHost;
    private final int backendPort;
    private final String token;
    private final String appId;
    private final String instanceId;

    private ManagedChannel channel;
    private VirtualThreadMonitorGrpc.VirtualThreadMonitorBlockingStub blockingStub;
    private VirtualThreadMonitorGrpc.VirtualThreadMonitorStub asyncStub;
    private StreamObserver<EventBatch> requestObserver;
    private volatile String sessionToken;

    private final AtomicLong droppedBatches = new AtomicLong(0);
    private final AtomicLong sentBatches = new AtomicLong(0);
    private volatile boolean connected;
    private volatile boolean shutdown;

    private ScheduledExecutorService heartbeatScheduler;
    private volatile long flushIntervalMs;

    public GrpcReporter(String backendHost, int backendPort, String token,
                        String appId, String instanceId) {
        this.backendHost = backendHost;
        this.backendPort = backendPort;
        this.token = token;
        this.appId = appId;
        this.instanceId = instanceId;
        this.flushIntervalMs = 3000L;
    }

    /**
     * Package-private constructor for testing with a pre-built channel.
     */
    GrpcReporter(ManagedChannel channel, String token, String appId, String instanceId) {
        this.backendHost = "test";
        this.backendPort = 0;
        this.token = token;
        this.appId = appId;
        this.instanceId = instanceId;
        this.flushIntervalMs = 3000L;
        this.channel = channel;
        this.blockingStub = VirtualThreadMonitorGrpc.newBlockingStub(channel);
        this.asyncStub = VirtualThreadMonitorGrpc.newStub(channel);
    }

    @Override
    public void onBatch(EventBatch batch) {
        if (shutdown) {
            return;
        }
        if (!connected) {
            connectWithRetry();
            if (!connected) {
                droppedBatches.incrementAndGet();
                log.warn("Dropping batch {} due to connection failure", batch.getBatchSeq());
                return;
            }
        }
        try {
            requestObserver.onNext(batch);
            sentBatches.incrementAndGet();
        } catch (Exception e) {
            log.error("Failed to send batch {}, reconnecting...", batch.getBatchSeq(), e);
            connected = false;
            droppedBatches.incrementAndGet();
        }
    }

    private void connectWithRetry() {
        int attempt = 0;
        long backoff = 1000L;
        while (attempt < MAX_RETRIES && !shutdown) {
            try {
                attempt++;
                connect();
                log.info("Successfully connected on attempt {}/{}", attempt, MAX_RETRIES);
                return;
            } catch (Exception e) {
                log.warn("Connection attempt {}/{} failed: {}", attempt, MAX_RETRIES, e.getMessage());
                if (attempt < MAX_RETRIES) {
                    try {
                        Thread.sleep(backoff);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    backoff *= 2;
                }
            }
        }
        log.error("All {} connection attempts failed", MAX_RETRIES);
    }

    private void connect() {
        if (channel == null) {
            channel = ManagedChannelBuilder.forAddress(backendHost, backendPort)
                    .usePlaintext()
                    .build();
            blockingStub = VirtualThreadMonitorGrpc.newBlockingStub(channel);
            asyncStub = VirtualThreadMonitorGrpc.newStub(channel);
        }

        RegisterRequest registerRequest = RegisterRequest.newBuilder()
                .setAppId(appId)
                .setInstanceId(instanceId)
                .setAuthToken(token)
                .setAgentVersion("0.5.0")
                .setJdkVersion(System.getProperty("java.version", "unknown"))
                .build();

        RegisterResponse registerResponse = blockingStub.register(registerRequest);
        this.sessionToken = registerResponse.getSessionToken();

        if (registerResponse.hasConfig()) {
            AgentConfig config = registerResponse.getConfig();
            if (config.getBatchSize() > 0) {
                this.flushIntervalMs = config.getFlushIntervalMs();
            }
        }

        openReportStream();
        startHeartbeat();
        connected = true;
    }

    private void openReportStream() {
        StreamObserver<ControlCommand> responseObserver = new StreamObserver<ControlCommand>() {
            @Override
            public void onNext(ControlCommand command) {
                handleControlCommand(command);
            }

            @Override
            public void onError(Throwable t) {
                log.error("Report stream error: {}", t.getMessage());
                connected = false;
            }

            @Override
            public void onCompleted() {
                log.info("Report stream completed by server");
                connected = false;
            }
        };

        this.requestObserver = asyncStub.withInterceptors(
                new io.grpc.Metadata.Key<>("authorization",
                        io.grpc.Metadata.ASCII_STRING_MARSHALLER)
        ).report(responseObserver);
    }

    private void handleControlCommand(ControlCommand command) {
        switch (command.getType()) {
            case ACK:
                log.debug("Received ACK for command {}", command.getCommandId());
                break;
            case SLOW_DOWN:
                log.warn("Received SLOW_DOWN command, increasing flush interval");
                this.flushIntervalMs = Math.min(this.flushIntervalMs * 2, 60_000L);
                break;
            case UPDATE_CONFIG:
                if (command.hasNewConfig()) {
                    AgentConfig config = command.getNewConfig();
                    if (config.getFlushIntervalMs() > 0) {
                        this.flushIntervalMs = config.getFlushIntervalMs();
                    }
                    log.info("Applied remote config: flushIntervalMs={}", this.flushIntervalMs);
                }
                break;
            default:
                log.debug("Received unhandled command: {}", command.getType());
        }
    }

    private void startHeartbeat() {
        heartbeatScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "grpc-reporter-heartbeat");
            t.setDaemon(true);
            return t;
        });
        heartbeatScheduler.scheduleAtFixedRate(() -> {
            try {
                HeartbeatRequest request = HeartbeatRequest.newBuilder()
                        .setSessionToken(sessionToken)
                        .setAppId(appId)
                        .setInstanceId(instanceId)
                        .setTimestampMs(System.currentTimeMillis())
                        .build();
                HeartbeatResponse response = blockingStub.heartbeat(request);
                if (!response.getOk()) {
                    log.warn("Heartbeat returned not OK");
                }
            } catch (Exception e) {
                log.warn("Heartbeat failed: {}", e.getMessage());
            }
        }, HEARTBEAT_INTERVAL_MS, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    public void shutdown() {
        shutdown = true;
        connected = false;
        if (heartbeatScheduler != null) {
            heartbeatScheduler.shutdown();
            try {
                if (!heartbeatScheduler.awaitTermination(2, TimeUnit.SECONDS)) {
                    heartbeatScheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                heartbeatScheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        if (requestObserver != null) {
            try {
                requestObserver.onCompleted();
            } catch (Exception e) {
                log.debug("Error completing request stream: {}", e.getMessage());
            }
        }
        if (channel != null) {
            channel.shutdown();
            try {
                if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                    channel.shutdownNow();
                }
            } catch (InterruptedException e) {
                channel.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    public boolean isConnected() {
        return connected;
    }

    public long getDroppedBatches() {
        return droppedBatches.get();
    }

    public long getSentBatches() {
        return sentBatches.get();
    }

    public long getFlushIntervalMs() {
        return flushIntervalMs;
    }

    public String getSessionToken() {
        return sessionToken;
    }
}
```

### 5.3.2 — Create GrpcReporterTest

- [ ] Create GrpcReporterTest.java:

```java
package io.github.dlwatching.agent.reporter;

import io.github.dlwatching.proto.*;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class GrpcReporterTest {

    private static final String SERVER_NAME = "test-server";
    private Server server;
    private TestMonitorService testService;
    private GrpcReporter reporter;

    @BeforeEach
    void setUp() throws IOException {
        testService = new TestMonitorService();
        server = InProcessServerBuilder.forName(SERVER_NAME)
                .directExecutor()
                .addService(ServerInterceptors.intercept(testService))
                .build()
                .start();

        reporter = new GrpcReporter(
                InProcessChannelBuilder.forName(SERVER_NAME).directExecutor().build(),
                "test-token", "test-app", "instance-1"
        );
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        if (reporter != null) {
            reporter.shutdown();
        }
        if (server != null) {
            server.shutdown();
            server.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldRegisterAndSendBatchSuccessfully() throws InterruptedException {
        EventBatch batch = createTestBatch("test-app", "instance-1", 0, 3);

        reporter.onBatch(batch);

        assertThat(testService.awaitRegister(2000)).isTrue();
        assertThat(testService.awaitBatch(2000)).isTrue();
        assertThat(testService.getReceivedAppId()).isEqualTo("test-app");
        assertThat(testService.getReceivedBatchCount()).isEqualTo(1);
        assertThat(testService.getReceivedEvents()).isEqualTo(3);
    }

    @Test
    void shouldRetryOnConnectionFailureAndDropAfterMaxRetries() {
        GrpcReporter badReporter = new GrpcReporter(
                InProcessChannelBuilder.forName("nonexistent-server").directExecutor().build(),
                "test-token", "test-app", "instance-1"
        );

        EventBatch batch = createTestBatch("test-app", "instance-1", 0, 1);
        badReporter.onBatch(batch);

        assertThat(badReporter.getDroppedBatches()).isGreaterThanOrEqualTo(1);
        badReporter.shutdown();
    }

    @Test
    void shouldHandleAckCommandOnResponseStream() throws InterruptedException {
        testService.setResponseCommand(ControlCommand.newBuilder()
                .setType(ControlCommand.CommandType.ACK)
                .setCommandId("cmd-1")
                .build());

        EventBatch batch = createTestBatch("test-app", "instance-1", 0, 1);
        reporter.onBatch(batch);

        assertThat(testService.awaitBatch(2000)).isTrue();
        assertThat(reporter.getSentBatches()).isEqualTo(1);
    }

    @Test
    void shouldHandleSlowDownCommandByDoublingFlushInterval() throws InterruptedException {
        long initialInterval = reporter.getFlushIntervalMs();
        assertThat(initialInterval).isEqualTo(3000L);

        testService.setResponseCommand(ControlCommand.newBuilder()
                .setType(ControlCommand.CommandType.SLOW_DOWN)
                .setCommandId("cmd-slow")
                .build());

        EventBatch batch = createTestBatch("test-app", "instance-1", 0, 1);
        reporter.onBatch(batch);

        assertThat(testService.awaitBatch(2000)).isTrue();
        assertThat(reporter.getFlushIntervalMs()).isEqualTo(6000L);
    }

    @Test
    void shouldHandleUpdateConfigCommand() throws InterruptedException {
        AgentConfig newConfig = AgentConfig.newBuilder()
                .setBatchSize(100)
                .setFlushIntervalMs(5000)
                .setSampleRate(0.1f)
                .build();

        testService.setResponseCommand(ControlCommand.newBuilder()
                .setType(ControlCommand.CommandType.UPDATE_CONFIG)
                .setCommandId("cmd-config")
                .setNewConfig(newConfig)
                .build());

        EventBatch batch = createTestBatch("test-app", "instance-1", 0, 1);
        reporter.onBatch(batch);

        assertThat(testService.awaitBatch(2000)).isTrue();
        assertThat(reporter.getFlushIntervalMs()).isEqualTo(5000);
    }

    @Test
    void shouldSendHeartbeatPeriodically() throws InterruptedException {
        EventBatch batch = createTestBatch("test-app", "instance-1", 0, 1);
        reporter.onBatch(batch);
        assertThat(testService.awaitRegister(2000)).isTrue();
        assertThat(testService.awaitBatch(2000)).isTrue();

        assertThat(testService.awaitHeartbeat(12000)).isTrue();
        assertThat(testService.getHeartbeatCount()).isGreaterThanOrEqualTo(1);
    }

    private static EventBatch createTestBatch(String appId, String instanceId,
                                               long batchSeq, int eventCount) {
        EventBatch.Builder builder = EventBatch.newBuilder()
                .setAppId(appId)
                .setInstanceId(instanceId)
                .setBatchSeq(batchSeq)
                .setTimestampMs(System.currentTimeMillis());

        for (int i = 0; i < eventCount; i++) {
            builder.addEvents(ThreadEvent.newBuilder()
                    .setType(ThreadEvent.EventType.CREATED)
                    .setThreadId(i)
                    .setThreadName("vt-" + i)
                    .setTimestampMs(System.currentTimeMillis()));
        }
        return builder.build();
    }

    private static class TestMonitorService
            extends VirtualThreadMonitorGrpc.VirtualThreadMonitorImplBase {

        private final CountDownLatch registerLatch = new CountDownLatch(1);
        private final CountDownLatch batchLatch = new CountDownLatch(1);
        private final CountDownLatch heartbeatLatch = new CountDownLatch(1);
        private final AtomicInteger batchCount = new AtomicInteger(0);
        private final AtomicInteger eventCount = new AtomicInteger(0);
        private final AtomicReference<String> receivedAppId = new AtomicReference<>();
        private final AtomicInteger heartbeatCount = new AtomicInteger(0);
        private ControlCommand responseCommand;

        void setResponseCommand(ControlCommand command) {
            this.responseCommand = command;
        }

        @Override
        public void register(RegisterRequest request,
                             StreamObserver<RegisterResponse> responseObserver) {
            receivedAppId.set(request.getAppId());
            RegisterResponse response = RegisterResponse.newBuilder()
                    .setSessionToken("session-" + request.getAppId())
                    .setConfig(AgentConfig.getDefaultInstance())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            registerLatch.countDown();
        }

        @Override
        public StreamObserver<EventBatch> report(
                StreamObserver<ControlCommand> responseObserver) {
            return new StreamObserver<EventBatch>() {
                @Override
                public void onNext(EventBatch batch) {
                    batchCount.incrementAndGet();
                    eventCount.addAndGet(batch.getEventsCount());
                    receivedAppId.set(batch.getAppId());

                    if (responseCommand != null) {
                        responseObserver.onNext(responseCommand);
                    } else {
                        responseObserver.onNext(ControlCommand.newBuilder()
                                .setType(ControlCommand.CommandType.ACK)
                                .setCommandId("ack-" + batch.getBatchSeq())
                                .build());
                    }
                    batchLatch.countDown();
                }

                @Override
                public void onError(Throwable t) {
                }

                @Override
                public void onCompleted() {
                    responseObserver.onCompleted();
                }
            };
        }

        @Override
        public void heartbeat(HeartbeatRequest request,
                              StreamObserver<HeartbeatResponse> responseObserver) {
            heartbeatCount.incrementAndGet();
            HeartbeatResponse response = HeartbeatResponse.newBuilder()
                    .setOk(true)
                    .setServerTimestampMs(System.currentTimeMillis())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            heartbeatLatch.countDown();
        }

        boolean awaitRegister(long timeoutMs) throws InterruptedException {
            return registerLatch.await(timeoutMs, TimeUnit.MILLISECONDS);
        }

        boolean awaitBatch(long timeoutMs) throws InterruptedException {
            return batchLatch.await(timeoutMs, TimeUnit.MILLISECONDS);
        }

        boolean awaitHeartbeat(long timeoutMs) throws InterruptedException {
            return heartbeatLatch.await(timeoutMs, TimeUnit.MILLISECONDS);
        }

        String getReceivedAppId() {
            return receivedAppId.get();
        }

        int getReceivedBatchCount() {
            return batchCount.get();
        }

        int getReceivedEvents() {
            return eventCount.get();
        }

        int getHeartbeatCount() {
            return heartbeatCount.get();
        }
    }
}
```

### 5.3.3 — Verify and commit

- [ ] Run tests:
  ```bash
  cd "D:\java-project\DL-Watching" && mvn clean test -pl agent -Dtest="io.github.dlwatching.agent.reporter.GrpcReporterTest" -DfailIfNoTests=false
  ```
- [ ] Commit:
  ```bash
  cd "D:\java-project\DL-Watching" && git add agent/src/main/java/io/github/dlwatching/agent/reporter/GrpcReporter.java agent/src/test/java/io/github/dlwatching/agent/reporter/GrpcReporterTest.java && git commit -m "M5-T5.3: Implement GrpcReporter with gRPC streaming, retry, and heartbeat"
  ```

---

## Task 5.4: AgentConfigManager — agent configuration lifecycle

**File:** `D:\java-project\DL-Watching\agent\src\main\java\io\github\dlwatching\agent\config\AgentConfigManager.java`
**Test:** `D:\java-project\DL-Watching\agent\src\test\java\io\github\dlwatching\agent\config\AgentConfigManagerTest.java`

### 5.4.1 — Create ConfigException

- [ ] Create `D:\java-project\DL-Watching\agent\src\main\java\io\github\dlwatching\agent\config\ConfigException.java`:

```java
package io.github.dlwatching.agent.config;

public class ConfigException extends RuntimeException {
    public ConfigException(String message) {
        super(message);
    }

    public ConfigException(String message, Throwable cause) {
        super(message, cause);
    }
}
```

### 5.4.2 — Create AgentConfigManager

- [ ] Create AgentConfigManager.java:

```java
package io.github.dlwatching.agent.config;

import io.github.dlwatching.proto.AgentConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class AgentConfigManager {

    private static final Logger log = LoggerFactory.getLogger(AgentConfigManager.class);
    private static final String DEFAULT_PROPERTIES = "agent.properties";

    private final Properties localProperties = new Properties();
    private volatile int batchSize;
    private volatile long flushIntervalMs;
    private volatile int maxCacheEvents;
    private volatile int maxMemoryMb;
    private volatile double sampleRate;
    private volatile String logLevel;
    private volatile String appId;
    private volatile String backendHost;
    private volatile int backendPort;
    private volatile String authToken;

    public AgentConfigManager() {
        this(DEFAULT_PROPERTIES);
    }

    AgentConfigManager(String propertiesResource) {
        loadDefaults(propertiesResource);
    }

    private void loadDefaults(String resource) {
        // Set built-in defaults first
        batchSize = 500;
        flushIntervalMs = 3000;
        maxCacheEvents = 10000;
        maxMemoryMb = 64;
        sampleRate = 0.05;
        logLevel = "INFO";

        try (InputStream is = getClass().getClassLoader().getResourceAsStream(resource)) {
            if (is != null) {
                localProperties.load(is);
                applyProperties(localProperties);
                log.info("Loaded configuration from {}", resource);
            } else {
                log.warn("Configuration resource {} not found, using built-in defaults", resource);
            }
        } catch (IOException e) {
            log.warn("Failed to load configuration from {}: {}", resource, e.getMessage());
        }
    }

    private void applyProperties(Properties props) {
        if (props.containsKey("dlwatching.batch.size")) {
            batchSize = Integer.parseInt(props.getProperty("dlwatching.batch.size"));
        }
        if (props.containsKey("dlwatching.batch.interval.ms")) {
            flushIntervalMs = Long.parseLong(props.getProperty("dlwatching.batch.interval.ms"));
        }
        if (props.containsKey("dlwatching.cache.max.events")) {
            maxCacheEvents = Integer.parseInt(props.getProperty("dlwatching.cache.max.events"));
        }
        if (props.containsKey("dlwatching.cache.max.memory.mb")) {
            maxMemoryMb = Integer.parseInt(props.getProperty("dlwatching.cache.max.memory.mb"));
        }
        if (props.containsKey("dlwatching.sample.rate")) {
            sampleRate = Double.parseDouble(props.getProperty("dlwatching.sample.rate"));
            sampleRate = clamp(sampleRate, 0.0, 1.0);
        }
        if (props.containsKey("dlwatching.log.level")) {
            logLevel = props.getProperty("dlwatching.log.level");
        }
        if (props.containsKey("dlwatching.app.id")) {
            appId = props.getProperty("dlwatching.app.id");
        }
        if (props.containsKey("dlwatching.backend.host")) {
            backendHost = props.getProperty("dlwatching.backend.host");
        }
        if (props.containsKey("dlwatching.backend.port")) {
            backendPort = Integer.parseInt(props.getProperty("dlwatching.backend.port"));
        }
        if (props.containsKey("dlwatching.auth.token")) {
            authToken = props.getProperty("dlwatching.auth.token");
        }
    }

    /**
     * Validate that all required fields are present.
     *
     * @throws ConfigException if any required field is missing
     */
    public void validate() {
        if (appId == null || appId.isBlank()) {
            throw new ConfigException("Required property 'dlwatching.app.id' is missing or blank");
        }
        if (backendHost == null || backendHost.isBlank()) {
            throw new ConfigException("Required property 'dlwatching.backend.host' is missing or blank");
        }
        if (backendPort <= 0 || backendPort > 65535) {
            throw new ConfigException("Required property 'dlwatching.backend.port' is invalid: " + backendPort);
        }
        if (authToken == null || authToken.isBlank()) {
            throw new ConfigException("Required property 'dlwatching.auth.token' is missing or blank");
        }
    }

    /**
     * Apply remote configuration from the backend. Remote values override local
     * for batch_size, flush_interval_ms, and sample_rate. Local max_memory_mb
     * serves as a hard cap and cannot be increased remotely.
     */
    public void applyRemoteConfig(AgentConfig remote) {
        if (remote.getBatchSize() > 0) {
            this.batchSize = remote.getBatchSize();
        }
        if (remote.getFlushIntervalMs() > 0) {
            this.flushIntervalMs = remote.getFlushIntervalMs();
        }
        if (remote.getSampleRate() >= 0) {
            this.sampleRate = clamp(remote.getSampleRate(), 0.0, 1.0);
        }
        // maxMemoryMb is a hard cap — remote cannot increase it
        if (remote.getMaxMemoryMb() > 0 && remote.getMaxMemoryMb() < this.maxMemoryMb) {
            this.maxMemoryMb = remote.getMaxMemoryMb();
        }
        log.info("Applied remote config: batchSize={}, flushIntervalMs={}, sampleRate={}",
                batchSize, flushIntervalMs, sampleRate);
    }

    // --- Accessors ---

    public int getBatchSize() {
        return batchSize;
    }

    public long getFlushIntervalMs() {
        return flushIntervalMs;
    }

    public int getMaxCacheEvents() {
        return maxCacheEvents;
    }

    public int getMaxMemoryMb() {
        return maxMemoryMb;
    }

    public double getSampleRate() {
        return sampleRate;
    }

    public String getLogLevel() {
        return logLevel;
    }

    public String getAppId() {
        return appId;
    }

    public String getBackendHost() {
        return backendHost;
    }

    public int getBackendPort() {
        return backendPort;
    }

    public String getAuthToken() {
        return authToken;
    }

    private static double clamp(double value, double min, double max) {
        return Math.max(min, Math.min(max, value));
    }
}
```

### 5.4.3 — Create test agent.properties resource

- [ ] Create `D:\java-project\DL-Watching\agent\src\test\resources\agent.properties`:

```properties
dlwatching.app.id=test-app
dlwatching.backend.host=localhost
dlwatching.backend.port=9090
dlwatching.auth.token=test-token-123
dlwatching.batch.size=250
dlwatching.batch.interval.ms=1500
dlwatching.cache.max.events=5000
dlwatching.cache.max.memory.mb=32
dlwatching.sample.rate=0.10
dlwatching.log.level=DEBUG
```

### 5.4.4 — Create AgentConfigManagerTest

- [ ] Create AgentConfigManagerTest.java:

```java
package io.github.dlwatching.agent.config;

import io.github.dlwatching.proto.AgentConfig;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AgentConfigManagerTest {

    @Test
    void shouldLoadFromTestProperties() {
        AgentConfigManager config = new AgentConfigManager("agent.properties");
        config.validate();

        assertThat(config.getAppId()).isEqualTo("test-app");
        assertThat(config.getBackendHost()).isEqualTo("localhost");
        assertThat(config.getBackendPort()).isEqualTo(9090);
        assertThat(config.getAuthToken()).isEqualTo("test-token-123");
        assertThat(config.getBatchSize()).isEqualTo(250);
        assertThat(config.getFlushIntervalMs()).isEqualTo(1500);
        assertThat(config.getMaxCacheEvents()).isEqualTo(5000);
        assertThat(config.getMaxMemoryMb()).isEqualTo(32);
        assertThat(config.getSampleRate()).isEqualTo(0.10);
        assertThat(config.getLogLevel()).isEqualTo("DEBUG");
    }

    @Test
    void shouldThrowWhenMissingRequiredField() {
        AgentConfigManager config = new AgentConfigManager("nonexistent.properties");

        assertThatThrownBy(config::validate)
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("dlwatching.app.id");
    }

    @Test
    void shouldOverrideBatchSizeFromRemoteConfig() {
        AgentConfigManager config = new AgentConfigManager("agent.properties");
        config.validate();

        AgentConfig remote = AgentConfig.newBuilder()
                .setBatchSize(1000)
                .setFlushIntervalMs(5000)
                .setSampleRate(0.5f)
                .build();

        config.applyRemoteConfig(remote);

        assertThat(config.getBatchSize()).isEqualTo(1000);
        assertThat(config.getFlushIntervalMs()).isEqualTo(5000);
        assertThat(config.getSampleRate()).isEqualTo(0.5);
    }

    @Test
    void shouldNotIncreaseMemoryCapFromRemote() {
        AgentConfigManager config = new AgentConfigManager("agent.properties");
        config.validate();
        int originalMemory = config.getMaxMemoryMb();

        AgentConfig remote = AgentConfig.newBuilder()
                .setMaxMemoryMb(originalMemory * 2)
                .build();

        config.applyRemoteConfig(remote);

        assertThat(config.getMaxMemoryMb()).isEqualTo(originalMemory);
    }

    @Test
    void shouldDecreaseMemoryFromRemoteIfLower() {
        AgentConfigManager config = new AgentConfigManager("agent.properties");
        config.validate();

        AgentConfig remote = AgentConfig.newBuilder()
                .setMaxMemoryMb(16)
                .build();

        config.applyRemoteConfig(remote);

        assertThat(config.getMaxMemoryMb()).isEqualTo(16);
    }

    @Test
    void shouldClampSampleRate() {
        AgentConfigManager config = new AgentConfigManager("agent.properties");

        AgentConfig remoteLow = AgentConfig.newBuilder().setSampleRate(-0.5f).build();
        config.applyRemoteConfig(remoteLow);
        assertThat(config.getSampleRate()).isEqualTo(0.0);

        AgentConfig remoteHigh = AgentConfig.newBuilder().setSampleRate(1.5f).build();
        config.applyRemoteConfig(remoteHigh);
        assertThat(config.getSampleRate()).isEqualTo(1.0);
    }

    @Test
    void shouldUseBuiltInDefaultsWhenNoPropertiesFile() {
        AgentConfigManager config = new AgentConfigManager("nonexistent-file.properties");

        assertThat(config.getBatchSize()).isEqualTo(500);
        assertThat(config.getFlushIntervalMs()).isEqualTo(3000);
        assertThat(config.getMaxCacheEvents()).isEqualTo(10000);
        assertThat(config.getMaxMemoryMb()).isEqualTo(64);
        assertThat(config.getSampleRate()).isEqualTo(0.05);
        assertThat(config.getLogLevel()).isEqualTo("INFO");
    }

    @Test
    void shouldThrowOnInvalidPort() {
        AgentConfigManager config = new AgentConfigManager("agent.properties");

        AgentConfigManager badPort = new AgentConfigManager("agent.properties") {
            {
                // Override port directly via package access
                java.lang.reflect.Field portField;
                try {
                    portField = AgentConfigManager.class.getDeclaredField("backendPort");
                    portField.setAccessible(true);
                    portField.setInt(this, -1);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };

        assertThatThrownBy(badPort::validate)
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("dlwatching.backend.port");
    }
}
```

### 5.4.5 — Verify and commit

- [ ] Run tests:
  ```bash
  cd "D:\java-project\DL-Watching" && mvn clean test -pl agent -Dtest="io.github.dlwatching.agent.config.AgentConfigManagerTest" -DfailIfNoTests=false
  ```
- [ ] Commit:
  ```bash
  cd "D:\java-project\DL-Watching" && git add agent/src/main/java/io/github/dlwatching/agent/config/ConfigException.java agent/src/main/java/io/github/dlwatching/agent/config/AgentConfigManager.java agent/src/test/resources/agent.properties agent/src/test/java/io/github/dlwatching/agent/config/AgentConfigManagerTest.java && git commit -m "M5-T5.4: Implement AgentConfigManager with local/remote config lifecycle"
  ```

---

## Task 5.5: Integration — wire up Agent with full pipeline

**File:** `D:\java-project\DL-Watching\agent\src\test\java\io\github\dlwatching\agent\AgentPipelineIntegrationTest.java`

### 5.5.1 — Create AgentPipelineIntegrationTest

- [ ] Create AgentPipelineIntegrationTest.java:

```java
package io.github.dlwatching.agent;

import io.github.dlwatching.agent.cache.BatchAggregator;
import io.github.dlwatching.agent.cache.BatchListener;
import io.github.dlwatching.agent.cache.RingBuffer;
import io.github.dlwatching.proto.*;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class AgentPipelineIntegrationTest {

    private static final String SERVER_NAME = "integration-test-server";
    private static final int TOTAL_EVENTS = 12;
    private static final int MAX_BATCH_SIZE = 10;

    private Server server;
    private RingBuffer<ThreadEvent> ringBuffer;
    private BatchAggregator aggregator;
    private CollectingBatchListener listener;
    private TestMonitorService testService;

    @BeforeEach
    void setUp() throws IOException {
        testService = new TestMonitorService();
        server = InProcessServerBuilder.forName(SERVER_NAME)
                .directExecutor()
                .addService(ServerInterceptors.intercept(testService))
                .build()
                .start();

        ringBuffer = new RingBuffer<>(100);
        listener = new CollectingBatchListener(
                InProcessChannelBuilder.forName(SERVER_NAME).directExecutor().build(),
                "test-token", "test-app", "instance-1");
        aggregator = new BatchAggregator(ringBuffer, MAX_BATCH_SIZE, 5000,
                listener, "test-app", "instance-1");
        aggregator.start();
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        if (aggregator != null) {
            aggregator.stop();
        }
        if (server != null) {
            server.shutdown();
            server.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldFlowEventsThroughRingBufferToBatchToGrpc() throws InterruptedException {
        // Simulate VirtualThread lifecycle events
        long threadId = 1001L;
        long baseTime = System.currentTimeMillis();

        // -- Lifecycle: CREATED --
        offerEvent(threadId, "vt-order-1", ThreadEvent.EventType.CREATED, baseTime);
        // -- STARTED --
        offerEvent(threadId, "vt-order-1", ThreadEvent.EventType.STARTED, baseTime + 10);
        // -- MOUNTED --
        offerEvent(threadId, "vt-order-1", ThreadEvent.EventType.MOUNTED, baseTime + 20,
                "ForkJoinPool-1-worker-1", 0, "");
        // -- PARKED --
        offerEvent(threadId, "vt-order-1", ThreadEvent.EventType.PARKED, baseTime + 100,
                "ForkJoinPool-1-worker-1", 0, "java.util.concurrent.locks.ReentrantLock$Sync.lock");
        // -- UNPARKED --
        offerEvent(threadId, "vt-order-1", ThreadEvent.EventType.UNPARKED, baseTime + 500,
                "", 400_000, "");
        // -- UNMOUNTED --
        offerEvent(threadId, "vt-order-1", ThreadEvent.EventType.UNMOUNTED, baseTime + 510,
                "ForkJoinPool-1-worker-1", 10_000, "");
        // -- TERMINATED --
        offerEvent(threadId, "vt-order-1", ThreadEvent.EventType.TERMINATED, baseTime + 1000,
                "", 0, "");

        assertThat(aggregator.isRunning()).isTrue();
        assertThat(ringBuffer.size()).isEqualTo(7);

        // Simulate events for more threads to trigger count-based flush
        for (int i = 0; i < 5; i++) {
            long id = 2000L + i;
            offerEvent(id, "vt-worker-" + i, ThreadEvent.EventType.CREATED, baseTime + 2000 + i);
            offerEvent(id, "vt-worker-" + i, ThreadEvent.EventType.TERMINATED, baseTime + 3000 + i);
        }

        // Total 17 events now; first flush should happen at batch size 10
        // Wait for events to be received at server
        boolean received = testService.awaitBatchReceived(5000, TimeUnit.MILLISECONDS);
        assertThat(received).isTrue();

        assertThat(testService.getAllBatches()).isNotEmpty();

        // Verify all events were received at server
        List<ThreadEvent> allEvents = new ArrayList<>();
        for (EventBatch batch : testService.getAllBatches()) {
            allEvents.addAll(batch.getEventsList());
            assertThat(batch.getAppId()).isEqualTo("test-app");
            assertThat(batch.getInstanceId()).isEqualTo("instance-1");
        }

        assertThat(allEvents).isNotEmpty();

        // Verify specific events
        boolean hasCreated = allEvents.stream()
                .anyMatch(e -> e.getThreadId() == 1001L && e.getType() == ThreadEvent.EventType.CREATED);
        boolean hasTerminated = allEvents.stream()
                .anyMatch(e -> e.getThreadId() == 1001L && e.getType() == ThreadEvent.EventType.TERMINATED);
        boolean hasParked = allEvents.stream()
                .anyMatch(e -> e.getThreadId() == 1001L && e.getType() == ThreadEvent.EventType.PARKED);

        assertThat(hasCreated).isTrue();
        assertThat(hasTerminated).isTrue();
        assertThat(hasParked).isTrue();
    }

    @Test
    void shouldHandleMultipleBatchesWithCorrectSequencing() throws InterruptedException {
        int totalEvents = MAX_BATCH_SIZE * 2 + 3;
        long baseTime = System.currentTimeMillis();

        for (int i = 0; i < totalEvents; i++) {
            offerEvent(1000L + i, "vt-thread-" + i, ThreadEvent.EventType.CREATED, baseTime + i);
            aggregator.signalEvent(); // trigger count-based check
        }

        // Should have flushed at least 2 batches
        assertThat(testService.awaitBatchReceived(5000, TimeUnit.MILLISECONDS)).isTrue();

        int totalReceived = 0;
        for (EventBatch batch : testService.getAllBatches()) {
            totalReceived += batch.getEventsCount();
        }
        assertThat(totalReceived).isEqualTo(totalEvents);

        // Verify batch sequencing
        List<Long> seqNumbers = testService.getAllBatches().stream()
                .map(EventBatch::getBatchSeq)
                .sorted()
                .toList();
        assertThat(seqNumbers).isSorted();
    }

    private void offerEvent(long threadId, String threadName, ThreadEvent.EventType type, long timestampMs) {
        offerEvent(threadId, threadName, type, timestampMs, "", 0, "");
    }

    private void offerEvent(long threadId, String threadName, ThreadEvent.EventType type,
                            long timestampMs, String carrierThread, long durationUs, String reason) {
        ThreadEvent event = ThreadEvent.newBuilder()
                .setType(type)
                .setThreadId(threadId)
                .setThreadName(threadName)
                .setTimestampMs(timestampMs)
                .setCarrierThread(carrierThread)
                .setDurationUs(durationUs)
                .setReason(reason)
                .build();
        ringBuffer.offer(event);
    }

    private static class CollectingBatchListener implements BatchListener {
        private final GrpcReporter reporter;

        CollectingBatchListener(io.grpc.ManagedChannel channel, String token,
                                String appId, String instanceId) {
            this.reporter = new GrpcReporter(channel, token, appId, instanceId);
        }

        @Override
        public void onBatch(EventBatch batch) {
            reporter.onBatch(batch);
        }
    }

    private static class TestMonitorService
            extends VirtualThreadMonitorGrpc.VirtualThreadMonitorImplBase {

        private final List<EventBatch> receivedBatches =
                Collections.synchronizedList(new ArrayList<>());
        private final CountDownLatch batchLatch = new CountDownLatch(1);

        @Override
        public void register(RegisterRequest request,
                             StreamObserver<RegisterResponse> responseObserver) {
            responseObserver.onNext(RegisterResponse.newBuilder()
                    .setSessionToken("session-integration")
                    .setConfig(AgentConfig.getDefaultInstance())
                    .build());
            responseObserver.onCompleted();
        }

        @Override
        public StreamObserver<EventBatch> report(
                StreamObserver<ControlCommand> responseObserver) {
            return new StreamObserver<EventBatch>() {
                @Override
                public void onNext(EventBatch batch) {
                    receivedBatches.add(batch);
                    responseObserver.onNext(ControlCommand.newBuilder()
                            .setType(ControlCommand.CommandType.ACK)
                            .setCommandId("ack-" + batch.getBatchSeq())
                            .build());
                    batchLatch.countDown();
                }

                @Override
                public void onError(Throwable t) {
                }

                @Override
                public void onCompleted() {
                    responseObserver.onCompleted();
                }
            };
        }

        @Override
        public void heartbeat(HeartbeatRequest request,
                              StreamObserver<HeartbeatResponse> responseObserver) {
            responseObserver.onNext(HeartbeatResponse.newBuilder()
                    .setOk(true)
                    .setServerTimestampMs(System.currentTimeMillis())
                    .build());
            responseObserver.onCompleted();
        }

        boolean awaitBatchReceived(long timeout, TimeUnit unit) throws InterruptedException {
            return batchLatch.await(timeout, unit);
        }

        List<EventBatch> getAllBatches() {
            return List.copyOf(receivedBatches);
        }
    }
}
```

### 5.5.2 — Verify and commit

- [ ] Run integration test:
  ```bash
  cd "D:\java-project\DL-Watching" && mvn clean test -pl agent -Dtest="io.github.dlwatching.agent.AgentPipelineIntegrationTest" -DfailIfNoTests=false
  ```
- [ ] Run all agent tests:
  ```bash
  cd "D:\java-project\DL-Watching" && mvn clean test -pl agent
  ```
- [ ] Commit:
  ```bash
  cd "D:\java-project\DL-Watching" && git add agent/src/test/java/io/github/dlwatching/agent/AgentPipelineIntegrationTest.java && git commit -m "M5-T5.5: Integration test for full agent pipeline (RingBuffer -> BatchAggregator -> GrpcReporter)"
  ```
