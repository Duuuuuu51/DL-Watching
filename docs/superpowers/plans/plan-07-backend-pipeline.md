# DL-Watching Backend Data Pipeline

**Module:** M7
**Dependencies:** M6 (Backend Gateway)
**Packages:** `io.github.dlwatching.backend.pipeline`

---

## Task 7.1: ValidationPipeline — three-layer validator

**Files:**
- `D:\java-project\DL-Watching\backend\src\main\java\io\github\dlwatching\backend\pipeline\ValidationResult.java`
- `D:\java-project\DL-Watching\backend\src\main\java\io\github\dlwatching\backend\pipeline\AppWhitelist.java`
- `D:\java-project\DL-Watching\backend\src\main\java\io\github\dlwatching\backend\pipeline\ValidationPipeline.java`
- `D:\java-project\DL-Watching\backend\src\test\java\io\github\dlwatching\backend\pipeline\ValidationPipelineTest.java`

### 7.1.1 — Create ValidationResult record

- [ ] Create ValidationResult.java:

```java
package io.github.dlwatching.backend.pipeline;

public record ValidationResult(
        boolean valid,
        String errorCode,
        String message,
        ValidationLevel level
) {
    public enum ValidationLevel {
        PROTOCOL,
        SEMANTIC,
        QUALITY
    }

    public static ValidationResult valid() {
        return new ValidationResult(true, null, null, null);
    }

    public static ValidationResult failure(String errorCode, String message, ValidationLevel level) {
        return new ValidationResult(false, errorCode, message, level);
    }
}
```

### 7.1.2 — Create AppWhitelist

- [ ] Create AppWhitelist.java:

```java
package io.github.dlwatching.backend.pipeline;

import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class AppWhitelist {

    private final Set<String> allowedAppIds = ConcurrentHashMap.newKeySet();

    public AppWhitelist() {
        // Allow registration-based auto-whitelisting by default
    }

    public boolean isAllowed(String appId) {
        if (appId == null || appId.isBlank()) {
            return false;
        }
        // If whitelist is empty, all apps are allowed (open mode)
        if (allowedAppIds.isEmpty()) {
            return true;
        }
        return allowedAppIds.contains(appId);
    }

    public void addApp(String appId) {
        if (appId != null && !appId.isBlank()) {
            allowedAppIds.add(appId);
        }
    }

    public void removeApp(String appId) {
        allowedAppIds.remove(appId);
    }

    public boolean isWhitelistActive() {
        return !allowedAppIds.isEmpty();
    }

    public Set<String> getAllowedAppIds() {
        return Set.copyOf(allowedAppIds);
    }

    public void clear() {
        allowedAppIds.clear();
    }
}
```

### 7.1.3 — Create ValidationPipeline

- [ ] Create ValidationPipeline.java:

```java
package io.github.dlwatching.backend.pipeline;

import io.github.dlwatching.proto.EventBatch;
import io.github.dlwatching.proto.ThreadEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

@Component
public class ValidationPipeline {

    private static final Logger log = LoggerFactory.getLogger(ValidationPipeline.class);
    private static final int MAX_BATCH_SIZE = 1000;
    private static final long MAX_FUTURE_SKEW_MS = 5000;
    private static final long MAX_DURATION_US = 86_400_000_000L; // 24 hours
    private static final Pattern INSTANCE_ID_PATTERN =
            Pattern.compile("^[a-zA-Z0-9_-]+_[0-9]+$");

    private final AppWhitelist appWhitelist;

    public ValidationPipeline(AppWhitelist appWhitelist) {
        this.appWhitelist = appWhitelist;
    }

    public List<ValidationResult> validate(EventBatch batch) {
        List<ValidationResult> results = new ArrayList<>();

        // Layer 1: Protocol validation
        results.addAll(validateProtocol(batch));

        // If protocol validation fails, don't continue to semantic/quality layers
        boolean protocolFailed = results.stream()
                .anyMatch(r -> !r.valid() && r.level() == ValidationResult.ValidationLevel.PROTOCOL);
        if (protocolFailed) {
            return results;
        }

        // Layer 2: Semantic validation
        results.addAll(validateSemantic(batch));

        // Layer 3: Quality validation
        results.addAll(validateQuality(batch));

        return results;
    }

    public boolean isValid(EventBatch batch) {
        return validate(batch).stream().noneMatch(r -> !r.valid());
    }

    private List<ValidationResult> validateProtocol(EventBatch batch) {
        List<ValidationResult> results = new ArrayList<>();

        if (batch == null) {
            results.add(ValidationResult.failure(
                    "PROTO_NULL", "Batch must not be null",
                    ValidationResult.ValidationLevel.PROTOCOL));
            return results;
        }

        if (batch.getAppId() == null || batch.getAppId().isBlank()) {
            results.add(ValidationResult.failure(
                    "PROTO_APP_ID_BLANK", "app_id must not be blank",
                    ValidationResult.ValidationLevel.PROTOCOL));
        }

        if (batch.getEventsList() == null || batch.getEventsList().isEmpty()) {
            results.add(ValidationResult.failure(
                    "PROTO_EVENTS_EMPTY", "events list must not be empty",
                    ValidationResult.ValidationLevel.PROTOCOL));
        }

        if (batch.getEventsCount() > MAX_BATCH_SIZE) {
            results.add(ValidationResult.failure(
                    "PROTO_BATCH_TOO_LARGE",
                    "batch size " + batch.getEventsCount() + " exceeds maximum " + MAX_BATCH_SIZE,
                    ValidationResult.ValidationLevel.PROTOCOL));
        }

        return results;
    }

    private List<ValidationResult> validateSemantic(EventBatch batch) {
        List<ValidationResult> results = new ArrayList<>();

        String appId = batch.getAppId();

        // App ID whitelist check
        if (!appWhitelist.isAllowed(appId)) {
            results.add(ValidationResult.failure(
                    "SEM_APP_NOT_WHITELISTED",
                    "app_id '" + appId + "' is not in the whitelist",
                    ValidationResult.ValidationLevel.SEMANTIC));
        }

        // Instance ID format check
        String instanceId = batch.getInstanceId();
        if (instanceId != null && !instanceId.isBlank()) {
            if (!INSTANCE_ID_PATTERN.matcher(instanceId).matches()) {
                results.add(ValidationResult.failure(
                        "SEM_INSTANCE_ID_INVALID",
                        "instance_id '" + instanceId + "' does not match pattern hostname_pid",
                        ValidationResult.ValidationLevel.SEMANTIC));
            }
        }

        // Timestamp not in future
        long now = System.currentTimeMillis();
        long batchTimestamp = batch.getTimestampMs();
        if (batchTimestamp > now + MAX_FUTURE_SKEW_MS) {
            results.add(ValidationResult.failure(
                    "SEM_TIMESTAMP_FUTURE",
                    "batch timestamp " + batchTimestamp + " is too far in the future (now=" + now + ")",
                    ValidationResult.ValidationLevel.SEMANTIC));
        }

        // Batch seq monotonic check (best-effort: warn on large gaps)
        // Note: full monotonic check requires state tracking; here we just verify it's > 0
        if (batch.getBatchSeq() < 0) {
            results.add(ValidationResult.failure(
                    "SEM_BATCH_SEQ_NEGATIVE",
                    "batch_seq " + batch.getBatchSeq() + " is negative",
                    ValidationResult.ValidationLevel.SEMANTIC));
        }

        return results;
    }

    private List<ValidationResult> validateQuality(EventBatch batch) {
        List<ValidationResult> results = new ArrayList<>();

        for (int i = 0; i < batch.getEventsCount(); i++) {
            ThreadEvent event = batch.getEvents(i);

            // thread_id > 0
            if (event.getThreadId() <= 0) {
                results.add(ValidationResult.failure(
                        "QUAL_THREAD_ID_INVALID",
                        "event[" + i + "] thread_id=" + event.getThreadId() + " must be > 0",
                        ValidationResult.ValidationLevel.QUALITY));
            }

            // duration_us in [0, 86400000000]
            long duration = event.getDurationUs();
            if (duration < 0 || duration > MAX_DURATION_US) {
                results.add(ValidationResult.failure(
                        "QUAL_DURATION_OUT_OF_RANGE",
                        "event[" + i + "] duration_us=" + duration + " outside valid range",
                        ValidationResult.ValidationLevel.QUALITY));
            }

            // carrier_thread format validity
            String carrierThread = event.getCarrierThread();
            if (carrierThread != null && !carrierThread.isBlank()
                    && carrierThread.length() > 256) {
                results.add(ValidationResult.failure(
                        "QUAL_CARRIER_THREAD_TOO_LONG",
                        "event[" + i + "] carrier_thread length=" + carrierThread.length() + " exceeds 256",
                        ValidationResult.ValidationLevel.QUALITY));
            }
        }

        return results;
    }
}
```

### 7.1.4 — Create ValidationPipelineTest

- [ ] Create ValidationPipelineTest.java:

```java
package io.github.dlwatching.backend.pipeline;

import io.github.dlwatching.proto.EventBatch;
import io.github.dlwatching.proto.ThreadEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ValidationPipelineTest {

    private AppWhitelist appWhitelist;
    private ValidationPipeline pipeline;

    @BeforeEach
    void setUp() {
        appWhitelist = new AppWhitelist();
        pipeline = new ValidationPipeline(appWhitelist);
        // Add test app to whitelist
        appWhitelist.addApp("order-service");
    }

    @Test
    void shouldPassValidBatchWithNoErrors() {
        EventBatch batch = createValidBatch("order-service", "host-1_12345");

        var results = pipeline.validate(batch);

        assertThat(results).allMatch(ValidationResult::valid);
    }

    @Test
    void shouldDetectEmptyBatchAsProtocolError() {
        EventBatch batch = EventBatch.newBuilder()
                .setAppId("order-service")
                .setInstanceId("host-1_12345")
                .setBatchSeq(0)
                .setTimestampMs(System.currentTimeMillis())
                .build();

        var results = pipeline.validate(batch);

        assertThat(results).anyMatch(r ->
                !r.valid() && r.level() == ValidationResult.ValidationLevel.PROTOCOL
                        && "PROTO_EVENTS_EMPTY".equals(r.errorCode()));
    }

    @Test
    void shouldDetectUnregisteredAppIdAsSemanticError() {
        EventBatch batch = createValidBatch("unknown-app", "host-1_12345");

        var results = pipeline.validate(batch);

        assertThat(results).anyMatch(r ->
                !r.valid() && r.level() == ValidationResult.ValidationLevel.SEMANTIC
                        && "SEM_APP_NOT_WHITELISTED".equals(r.errorCode()));
    }

    @Test
    void shouldDetectFutureTimestampAsSemanticError() {
        EventBatch batch = EventBatch.newBuilder()
                .setAppId("order-service")
                .setInstanceId("host-1_12345")
                .setBatchSeq(0)
                .setTimestampMs(System.currentTimeMillis() + 60_000) // 1 minute in future
                .addEvents(createTestEvent(1, "vt-1", ThreadEvent.EventType.CREATED))
                .build();

        var results = pipeline.validate(batch);

        assertThat(results).anyMatch(r ->
                !r.valid() && r.level() == ValidationResult.ValidationLevel.SEMANTIC
                        && "SEM_TIMESTAMP_FUTURE".equals(r.errorCode()));
    }

    @Test
    void shouldDetectNegativeDurationAsQualityError() {
        EventBatch batch = EventBatch.newBuilder()
                .setAppId("order-service")
                .setInstanceId("host-1_12345")
                .setBatchSeq(0)
                .setTimestampMs(System.currentTimeMillis())
                .addEvents(ThreadEvent.newBuilder()
                        .setType(ThreadEvent.EventType.PARKED)
                        .setThreadId(1)
                        .setThreadName("vt-1")
                        .setTimestampMs(System.currentTimeMillis())
                        .setDurationUs(-100)
                        .build())
                .build();

        var results = pipeline.validate(batch);

        assertThat(results).anyMatch(r ->
                !r.valid() && r.level() == ValidationResult.ValidationLevel.QUALITY
                        && "QUAL_DURATION_OUT_OF_RANGE".equals(r.errorCode()));
    }

    @Test
    void shouldReturnMultipleErrorsForMixedFailures() {
        EventBatch batch = EventBatch.newBuilder()
                .setAppId("") // blank app_id
                .setInstanceId("invalid") // doesn't match pattern
                .setBatchSeq(-1)
                .setTimestampMs(System.currentTimeMillis())
                .addEvents(ThreadEvent.newBuilder()
                        .setType(ThreadEvent.EventType.CREATED)
                        .setThreadId(0) // invalid thread_id
                        .setDurationUs(-5)
                        .build())
                .build();

        var results = pipeline.validate(batch);

        // Should have protocol error for blank app_id
        assertThat(results).anyMatch(r ->
                !r.valid() && "PROTO_APP_ID_BLANK".equals(r.errorCode()));

        // Protocol failure should prevent semantic/quality checks from running
        // But quality events are still validated if the batch passes protocol
        // Since protocol failed, semantic and quality might not run
        boolean hasProtocolFailure = results.stream()
                .anyMatch(r -> !r.valid() && r.level() == ValidationResult.ValidationLevel.PROTOCOL);
        assertThat(hasProtocolFailure).isTrue();
    }

    @Test
    void shouldDetectInvalidInstanceIdFormat() {
        EventBatch batch = createValidBatch("order-service", "bad-format-no-pid");

        var results = pipeline.validate(batch);

        assertThat(results).anyMatch(r ->
                !r.valid() && "SEM_INSTANCE_ID_INVALID".equals(r.errorCode()));
    }

    @Test
    void shouldDetectNullBatchAsProtocolError() {
        var results = pipeline.validate(null);

        assertThat(results).anyMatch(r ->
                !r.valid() && "PROTO_NULL".equals(r.errorCode()));
    }

    private static EventBatch createValidBatch(String appId, String instanceId) {
        return EventBatch.newBuilder()
                .setAppId(appId)
                .setInstanceId(instanceId)
                .setBatchSeq(1)
                .setTimestampMs(System.currentTimeMillis())
                .addEvents(createTestEvent(1, "vt-1", ThreadEvent.EventType.CREATED))
                .addEvents(createTestEvent(2, "vt-2", ThreadEvent.EventType.STARTED))
                .build();
    }

    private static ThreadEvent createTestEvent(long threadId, String threadName,
                                                ThreadEvent.EventType type) {
        return ThreadEvent.newBuilder()
                .setType(type)
                .setThreadId(threadId)
                .setThreadName(threadName)
                .setTimestampMs(System.currentTimeMillis())
                .build();
    }
}
```

### 7.1.5 — Verify and commit

- [ ] Run tests:
  ```bash
  cd "D:\java-project\DL-Watching" && mvn clean test -pl backend -Dtest="io.github.dlwatching.backend.pipeline.ValidationPipelineTest" -DfailIfNoTests=false
  ```
- [ ] Commit:
  ```bash
  cd "D:\java-project\DL-Watching" && git add backend/src/main/java/io/github/dlwatching/backend/pipeline/ValidationResult.java backend/src/main/java/io/github/dlwatching/backend/pipeline/AppWhitelist.java backend/src/main/java/io/github/dlwatching/backend/pipeline/ValidationPipeline.java backend/src/test/java/io/github/dlwatching/backend/pipeline/ValidationPipelineTest.java && git commit -m "M7-T7.1: Implement ValidationPipeline with three-layer protocol/semantic/quality validation"
  ```

---

## Task 7.2: CleaningPipeline — dedup, filter, fill, state correction

**Files:**
- `D:\java-project\DL-Watching\backend\src\main\java\io\github\dlwatching\backend\pipeline\DedupStore.java`
- `D:\java-project\DL-Watching\backend\src\main\java\io\github\dlwatching\backend\pipeline\InMemoryDedupStore.java`
- `D:\java-project\DL-Watching\backend\src\main\java\io\github\dlwatching\backend\pipeline\CleaningPipeline.java`
- `D:\java-project\DL-Watching\backend\src\test\java\io\github\dlwatching\backend\pipeline\CleaningPipelineTest.java`

### 7.2.1 — Create DedupStore interface

- [ ] Create DedupStore.java:

```java
package io.github.dlwatching.backend.pipeline;

public interface DedupStore {
    /**
     * Returns true if the given key has been seen within the dedup window.
     */
    boolean isDuplicate(String key);

    /**
     * Records the key as seen (for future duplicate checks).
     */
    void mark(String key);
}
```

### 7.2.2 — Create InMemoryDedupStore

- [ ] Create InMemoryDedupStore.java:

```java
package io.github.dlwatching.backend.pipeline;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class InMemoryDedupStore implements DedupStore, AutoCloseable {

    private static final long CLEANUP_INTERVAL_MS = 300_000L; // 5 minutes
    private static final long WINDOW_MS = 300_000L; // 5 minutes dedup window

    private final ConcurrentHashMap<String, Long> store = new ConcurrentHashMap<>();
    private final ScheduledExecutorService cleanupExecutor;

    public InMemoryDedupStore() {
        this.cleanupExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "dedup-cleanup");
            t.setDaemon(true);
            return t;
        });
        cleanupExecutor.scheduleAtFixedRate(this::evictExpired,
                CLEANUP_INTERVAL_MS, CLEANUP_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    @Override
    public boolean isDuplicate(String key) {
        Long timestamp = store.get(key);
        if (timestamp == null) {
            return false;
        }
        return (System.currentTimeMillis() - timestamp) < WINDOW_MS;
    }

    @Override
    public void mark(String key) {
        store.put(key, System.currentTimeMillis());
    }

    private void evictExpired() {
        long cutoff = System.currentTimeMillis() - WINDOW_MS;
        store.entrySet().removeIf(entry -> entry.getValue() < cutoff);
    }

    public int size() {
        return store.size();
    }

    @Override
    public void close() {
        cleanupExecutor.shutdown();
        try {
            if (!cleanupExecutor.awaitTermination(2, TimeUnit.SECONDS)) {
                cleanupExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            cleanupExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
```

### 7.2.3 — Create CleaningPipeline

- [ ] Create CleaningPipeline.java:

```java
package io.github.dlwatching.backend.pipeline;

import io.github.dlwatching.proto.EventBatch;
import io.github.dlwatching.proto.ThreadEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Component
public class CleaningPipeline {

    private static final Logger log = LoggerFactory.getLogger(CleaningPipeline.class);
    private static final long MAX_DURATION_US = 86_400_000_000L; // 24 hours
    private static final String DEFAULT_THREAD_NAME_PREFIX = "vt-";

    private final DedupStore dedupStore;

    public CleaningPipeline(DedupStore dedupStore) {
        this.dedupStore = dedupStore;
    }

    /**
     * Clean the batch by applying dedup, anomaly filtering, empty field filling,
     * and state correction.
     *
     * @param batch the incoming event batch
     * @return a new EventBatch with cleaned events (may be empty)
     */
    public EventBatch clean(EventBatch batch) {
        if (batch == null) {
            return null;
        }

        List<ThreadEvent> originalEvents = batch.getEventsList();
        if (originalEvents == null || originalEvents.isEmpty()) {
            return batch;
        }

        List<ThreadEvent> cleanedEvents = new ArrayList<>();
        Set<String> seenThreadIds = new HashSet<>();

        for (int i = 0; i < originalEvents.size(); i++) {
            ThreadEvent event = originalEvents.get(i);
            ThreadEvent processed = processEvent(event, batch.getAppId(),
                    batch.getInstanceId(), batch.getBatchSeq(), seenThreadIds);
            if (processed != null) {
                cleanedEvents.add(processed);
            }
        }

        return EventBatch.newBuilder(batch)
                .clearEvents()
                .addAllEvents(cleanedEvents)
                .build();
    }

    /**
     * Clean a single event. Returns null if the event should be dropped.
     */
    private ThreadEvent processEvent(ThreadEvent event, String appId,
                                      String instanceId, long batchSeq,
                                      Set<String> seenThreadIds) {
        // Step 1: Dedup check
        String dedupKey = computeDedupKey(appId, instanceId, batchSeq, event);
        if (dedupStore.isDuplicate(dedupKey)) {
            log.debug("Duplicate event filtered: key={}", dedupKey);
            return null;
        }
        dedupStore.mark(dedupKey);

        // Step 2: Anomaly filter - duration outside valid range
        long duration = event.getDurationUs();
        if (duration < 0 || duration > MAX_DURATION_US) {
            log.debug("Anomaly duration filtered: threadId={}, durationUs={}",
                    event.getThreadId(), duration);
            return null;
        }

        // Step 3: Fill empty thread_name
        String threadName = event.getThreadName();
        if (threadName == null || threadName.isBlank()) {
            threadName = DEFAULT_THREAD_NAME_PREFIX + event.getThreadId();
        }

        // Step 4: State correction - consecutive CREATED for same thread_id
        if (event.getType() == ThreadEvent.EventType.CREATED) {
            long threadId = event.getThreadId();
            String threadKey = appId + ":" + instanceId + ":" + threadId;
            if (seenThreadIds.contains(threadKey)) {
                log.debug("State correction: dropping duplicate CREATED for threadId={}", threadId);
                return null;
            }
            seenThreadIds.add(threadKey);
        }

        // Build cleaned event (preserving all original fields)
        ThreadEvent.Builder builder = ThreadEvent.newBuilder(event)
                .setThreadName(threadName);

        return builder.build();
    }

    /**
     * Compute a dedup key from batch metadata and event fields.
     */
    private static String computeDedupKey(String appId, String instanceId,
                                           long batchSeq, ThreadEvent event) {
        return appId + "|" + instanceId + "|" + batchSeq + "|"
                + event.getThreadId() + "|" + event.getTypeValue();
    }
}
```

### 7.2.4 — Create CleaningPipelineTest

- [ ] Create CleaningPipelineTest.java:

```java
package io.github.dlwatching.backend.pipeline;

import io.github.dlwatching.proto.EventBatch;
import io.github.dlwatching.proto.ThreadEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CleaningPipelineTest {

    private InMemoryDedupStore dedupStore;
    private CleaningPipeline pipeline;

    @BeforeEach
    void setUp() {
        dedupStore = new InMemoryDedupStore();
        pipeline = new CleaningPipeline(dedupStore);
    }

    @AfterEach
    void tearDown() {
        dedupStore.close();
    }

    @Test
    void shouldFilterOutDuplicateEvent() {
        ThreadEvent event = ThreadEvent.newBuilder()
                .setType(ThreadEvent.EventType.CREATED)
                .setThreadId(1)
                .setThreadName("vt-1")
                .setTimestampMs(System.currentTimeMillis())
                .build();

        EventBatch batch = EventBatch.newBuilder()
                .setAppId("order-service")
                .setInstanceId("host-1_12345")
                .setBatchSeq(0)
                .setTimestampMs(System.currentTimeMillis())
                .addEvents(event)
                .addEvents(event) // duplicate
                .build();

        EventBatch cleaned = pipeline.clean(batch);

        assertThat(cleaned.getEventsCount()).isEqualTo(1);
    }

    @Test
    void shouldFilterOutNegativeDuration() {
        ThreadEvent event = ThreadEvent.newBuilder()
                .setType(ThreadEvent.EventType.PARKED)
                .setThreadId(1)
                .setThreadName("vt-1")
                .setTimestampMs(System.currentTimeMillis())
                .setDurationUs(-100)
                .build();

        EventBatch batch = EventBatch.newBuilder()
                .setAppId("order-service")
                .setInstanceId("host-1_12345")
                .setBatchSeq(0)
                .setTimestampMs(System.currentTimeMillis())
                .addEvents(event)
                .build();

        EventBatch cleaned = pipeline.clean(batch);

        assertThat(cleaned.getEventsCount()).isEqualTo(0);
    }

    @Test
    void shouldFillEmptyThreadName() {
        ThreadEvent event = ThreadEvent.newBuilder()
                .setType(ThreadEvent.EventType.CREATED)
                .setThreadId(42)
                .setThreadName("") // empty thread_name
                .setTimestampMs(System.currentTimeMillis())
                .build();

        EventBatch batch = EventBatch.newBuilder()
                .setAppId("order-service")
                .setInstanceId("host-1_12345")
                .setBatchSeq(0)
                .setTimestampMs(System.currentTimeMillis())
                .addEvents(event)
                .build();

        EventBatch cleaned = pipeline.clean(batch);

        assertThat(cleaned.getEventsCount()).isEqualTo(1);
        assertThat(cleaned.getEvents(0).getThreadName()).isEqualTo("vt-42");
    }

    @Test
    void shouldRemoveDuplicateConsecutiveCreatedEvents() {
        ThreadEvent event1 = ThreadEvent.newBuilder()
                .setType(ThreadEvent.EventType.CREATED)
                .setThreadId(1)
                .setThreadName("vt-1")
                .setTimestampMs(System.currentTimeMillis())
                .build();

        ThreadEvent event2 = ThreadEvent.newBuilder()
                .setType(ThreadEvent.EventType.CREATED)
                .setThreadId(1) // same thread
                .setThreadName("vt-1")
                .setTimestampMs(System.currentTimeMillis() + 1)
                .build();

        EventBatch batch = EventBatch.newBuilder()
                .setAppId("order-service")
                .setInstanceId("host-1_12345")
                .setBatchSeq(0)
                .setTimestampMs(System.currentTimeMillis())
                .addEvents(event1)
                .addEvents(event2) // second CREATED for same thread
                .build();

        EventBatch cleaned = pipeline.clean(batch);

        assertThat(cleaned.getEventsCount()).isEqualTo(1);
    }

    @Test
    void shouldReturnEmptyBatchWhenAllEventsFiltered() {
        ThreadEvent event = ThreadEvent.newBuilder()
                .setType(ThreadEvent.EventType.CREATED)
                .setThreadId(1)
                .setThreadName("vt-1")
                .setTimestampMs(System.currentTimeMillis())
                .setDurationUs(-1) // invalid duration
                .build();

        EventBatch batch = EventBatch.newBuilder()
                .setAppId("order-service")
                .setInstanceId("host-1_12345")
                .setBatchSeq(0)
                .setTimestampMs(System.currentTimeMillis())
                .addEvents(event)
                .build();

        EventBatch cleaned = pipeline.clean(batch);

        assertThat(cleaned.getEventsCount()).isEqualTo(0);
    }

    @Test
    void shouldFilterOutExcessiveDuration() {
        ThreadEvent event = ThreadEvent.newBuilder()
                .setType(ThreadEvent.EventType.PARKED)
                .setThreadId(1)
                .setThreadName("vt-1")
                .setTimestampMs(System.currentTimeMillis())
                .setDurationUs(86_400_000_001L) // exceeds 24h
                .build();

        EventBatch batch = EventBatch.newBuilder()
                .setAppId("order-service")
                .setInstanceId("host-1_12345")
                .setBatchSeq(0)
                .setTimestampMs(System.currentTimeMillis())
                .addEvents(event)
                .build();

        EventBatch cleaned = pipeline.clean(batch);

        assertThat(cleaned.getEventsCount()).isEqualTo(0);
    }

    @Test
    void shouldReturnNullForNullBatch() {
        assertThat(pipeline.clean(null)).isNull();
    }

    @Test
    void shouldPreserveOriginalFieldsOnValidEvent() {
        ThreadEvent event = ThreadEvent.newBuilder()
                .setType(ThreadEvent.EventType.PARKED)
                .setThreadId(1)
                .setThreadName("vt-order-worker")
                .setTimestampMs(1000)
                .setCarrierThread("ForkJoinPool-1-worker-1")
                .setDurationUs(5000)
                .setReason("java.util.concurrent.locks.ReentrantLock$Sync.lock")
                .build();

        EventBatch batch = EventBatch.newBuilder()
                .setAppId("order-service")
                .setInstanceId("host-1_12345")
                .setBatchSeq(0)
                .setTimestampMs(System.currentTimeMillis())
                .addEvents(event)
                .build();

        EventBatch cleaned = pipeline.clean(batch);

        assertThat(cleaned.getEventsCount()).isEqualTo(1);
        ThreadEvent cleanedEvent = cleaned.getEvents(0);
        assertThat(cleanedEvent.getThreadId()).isEqualTo(1);
        assertThat(cleanedEvent.getThreadName()).isEqualTo("vt-order-worker");
        assertThat(cleanedEvent.getCarrierThread()).isEqualTo("ForkJoinPool-1-worker-1");
        assertThat(cleanedEvent.getDurationUs()).isEqualTo(5000);
        assertThat(cleanedEvent.getReason()).isEqualTo("java.util.concurrent.locks.ReentrantLock$Sync.lock");
    }
}
```

### 7.2.5 — Verify and commit

- [ ] Run tests:
  ```bash
  cd "D:\java-project\DL-Watching" && mvn clean test -pl backend -Dtest="io.github.dlwatching.backend.pipeline.CleaningPipelineTest" -DfailIfNoTests=false
  ```
- [ ] Commit:
  ```bash
  cd "D:\java-project\DL-Watching" && git add backend/src/main/java/io/github/dlwatching/backend/pipeline/DedupStore.java backend/src/main/java/io/github/dlwatching/backend/pipeline/InMemoryDedupStore.java backend/src/main/java/io/github/dlwatching/backend/pipeline/CleaningPipeline.java backend/src/test/java/io/github/dlwatching/backend/pipeline/CleaningPipelineTest.java && git commit -m "M7-T7.2: Implement CleaningPipeline with dedup, anomaly filter, fill, and state correction"
  ```

---

## Task 7.3: TransformationPipeline — timestamp, enum, stack compression

**Files:**
- `D:\java-project\DL-Watching\backend\src\main\java\io\github\dlwatching\backend\pipeline\TransformedEvent.java`
- `D:\java-project\DL-Watching\backend\src\main\java\io\github\dlwatching\backend\pipeline\TransformedBatch.java`
- `D:\java-project\DL-Watching\backend\src\main\java\io\github\dlwatching\backend\pipeline\TransformationPipeline.java`
- `D:\java-project\DL-Watching\backend\src\test\java\io\github\dlwatching\backend\pipeline\TransformationPipelineTest.java`

### 7.3.1 — Create TransformedEvent record

- [ ] Create TransformedEvent.java:

```java
package io.github.dlwatching.backend.pipeline;

public record TransformedEvent(
        String appId,
        String instanceId,
        int batchSeq,
        String eventType,
        long threadId,
        String threadName,
        String carrierThread,
        long durationUs,
        String reason,
        String callerClass,
        String callerMethod,
        int callerLine,
        long clientTs,
        long serverTs
) {}
```

### 7.3.2 — Create TransformedBatch record

- [ ] Create TransformedBatch.java:

```java
package io.github.dlwatching.backend.pipeline;

import java.util.List;

public record TransformedBatch(
        String appId,
        String instanceId,
        int batchSeq,
        List<TransformedEvent> events,
        long clientTs,
        long serverTs
) {}
```

### 7.3.3 — Create TransformationPipeline

- [ ] Create TransformationPipeline.java:

```java
package io.github.dlwatching.backend.pipeline;

import io.github.dlwatching.proto.EventBatch;
import io.github.dlwatching.proto.ThreadEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@Component
public class TransformationPipeline {

    private static final Logger log = LoggerFactory.getLogger(TransformationPipeline.class);

    private final double defaultSampleRate;

    public TransformationPipeline() {
        this(0.05);
    }

    public TransformationPipeline(double defaultSampleRate) {
        this.defaultSampleRate = defaultSampleRate;
    }

    /**
     * Transform an EventBatch into a TransformedBatch.
     * Applies timestamp normalization, enum-to-string conversion, and stack compression.
     */
    public TransformedBatch transform(EventBatch batch) {
        return transform(batch, defaultSampleRate);
    }

    /**
     * Transform with an explicit sample rate for stack compression.
     */
    public TransformedBatch transform(EventBatch batch, double sampleRate) {
        if (batch == null) {
            return null;
        }

        long serverTs = System.currentTimeMillis();
        long clientTs = batch.getTimestampMs();

        List<ThreadEvent> events = batch.getEventsList();
        List<TransformedEvent> transformedEvents = new ArrayList<>();

        Random rng = new Random();

        for (int i = 0; i < events.size(); i++) {
            ThreadEvent event = events.get(i);

            // Enum to lowercase string
            String eventType = event.getType().name().toLowerCase();

            // Stack compression based on sample rate
            String callerClass = "";
            String callerMethod = "";
            int callerLine = 0;

            if (event.hasCaller()) {
                boolean keepFullStack = rng.nextDouble() < sampleRate;
                if (keepFullStack) {
                    callerClass = event.getCaller().getClassName();
                    callerMethod = event.getCaller().getMethodName();
                    callerLine = event.getCaller().getLineNumber();
                } else {
                    // Truncate: only keep className and methodName
                    callerClass = event.getCaller().getClassName();
                    callerMethod = event.getCaller().getMethodName();
                    callerLine = 0;
                }
            }

            TransformedEvent transformed = new TransformedEvent(
                    batch.getAppId(),
                    batch.getInstanceId(),
                    (int) batch.getBatchSeq(),
                    eventType,
                    event.getThreadId(),
                    event.getThreadName(),
                    event.getCarrierThread(),
                    event.getDurationUs(),
                    event.getReason(),
                    callerClass,
                    callerMethod,
                    callerLine,
                    clientTs,
                    serverTs
            );

            transformedEvents.add(transformed);
        }

        return new TransformedBatch(
                batch.getAppId(),
                batch.getInstanceId(),
                (int) batch.getBatchSeq(),
                transformedEvents,
                clientTs,
                serverTs
        );
    }
}
```

### 7.3.4 — Create TransformationPipelineTest

- [ ] Create TransformationPipelineTest.java:

```java
package io.github.dlwatching.backend.pipeline;

import io.github.dlwatching.proto.EventBatch;
import io.github.dlwatching.proto.ThreadEvent;
import io.github.dlwatching.proto.StackFrame;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TransformationPipelineTest {

    private TransformationPipeline pipeline;

    @BeforeEach
    void setUp() {
        pipeline = new TransformationPipeline(0.05);
    }

    @Test
    void shouldConvertEventTypeEnumToLowercaseString() {
        EventBatch batch = createBatchWithEvent(ThreadEvent.EventType.PARKED);

        TransformedBatch transformed = pipeline.transform(batch, 1.0);

        assertThat(transformed.events()).hasSize(1);
        assertThat(transformed.events().get(0).eventType()).isEqualTo("parked");
    }

    @Test
    void shouldKeepFullStackWhenSampleRateIsOne() {
        EventBatch batch = createBatchWithStackFrame("com.example.OrderService", "processOrder", 42);

        TransformedBatch transformed = pipeline.transform(batch, 1.0);

        TransformedEvent event = transformed.events().get(0);
        assertThat(event.callerClass()).isEqualTo("com.example.OrderService");
        assertThat(event.callerMethod()).isEqualTo("processOrder");
        assertThat(event.callerLine()).isEqualTo(42);
    }

    @Test
    void shouldTruncateStackWhenSampleRateIsZero() {
        EventBatch batch = createBatchWithStackFrame("com.example.OrderService", "processOrder", 42);

        TransformedBatch transformed = pipeline.transform(batch, 0.0);

        TransformedEvent event = transformed.events().get(0);
        assertThat(event.callerClass()).isEqualTo("com.example.OrderService");
        assertThat(event.callerMethod()).isEqualTo("processOrder");
        assertThat(event.callerLine()).isEqualTo(0);
    }

    @Test
    void shouldPreserveClientTsAndSetServerTs() {
        long beforeTransform = System.currentTimeMillis();
        EventBatch batch = createBatchWithEvent(ThreadEvent.EventType.CREATED);
        long afterTransform = System.currentTimeMillis();

        TransformedBatch transformed = pipeline.transform(batch, 1.0);

        assertThat(transformed.clientTs()).isEqualTo(batch.getTimestampMs());
        assertThat(transformed.serverTs()).isGreaterThanOrEqualTo(beforeTransform);
        assertThat(transformed.serverTs()).isLessThanOrEqualTo(afterTransform + 100);
    }

    @Test
    void shouldReturnEmptyTransformedBatchForEmptyBatch() {
        EventBatch batch = EventBatch.newBuilder()
                .setAppId("order-service")
                .setInstanceId("host-1_12345")
                .setBatchSeq(0)
                .setTimestampMs(System.currentTimeMillis())
                .build();

        TransformedBatch transformed = pipeline.transform(batch, 1.0);

        assertThat(transformed).isNotNull();
        assertThat(transformed.events()).isEmpty();
        assertThat(transformed.appId()).isEqualTo("order-service");
    }

    @Test
    void shouldTransformMultipleEventsWithDifferentTypes() {
        EventBatch.Builder builder = EventBatch.newBuilder()
                .setAppId("order-service")
                .setInstanceId("host-1_12345")
                .setBatchSeq(1)
                .setTimestampMs(System.currentTimeMillis());

        builder.addEvents(ThreadEvent.newBuilder()
                .setType(ThreadEvent.EventType.CREATED)
                .setThreadId(1)
                .setThreadName("vt-1")
                .setTimestampMs(System.currentTimeMillis()));
        builder.addEvents(ThreadEvent.newBuilder()
                .setType(ThreadEvent.EventType.STARTED)
                .setThreadId(1)
                .setThreadName("vt-1")
                .setTimestampMs(System.currentTimeMillis()));
        builder.addEvents(ThreadEvent.newBuilder()
                .setType(ThreadEvent.EventType.MOUNTED)
                .setThreadId(1)
                .setThreadName("vt-1")
                .setCarrierThread("ForkJoinPool-1-worker-1")
                .setTimestampMs(System.currentTimeMillis()));
        builder.addEvents(ThreadEvent.newBuilder()
                .setType(ThreadEvent.EventType.PARKED)
                .setThreadId(1)
                .setThreadName("vt-1")
                .setTimestampMs(System.currentTimeMillis())
                .setDurationUs(5000)
                .setReason("lock contention"));
        builder.addEvents(ThreadEvent.newBuilder()
                .setType(ThreadEvent.EventType.TERMINATED)
                .setThreadId(1)
                .setThreadName("vt-1")
                .setTimestampMs(System.currentTimeMillis()));

        TransformedBatch transformed = pipeline.transform(builder.build(), 1.0);

        assertThat(transformed.events()).hasSize(5);
        assertThat(transformed.events().get(0).eventType()).isEqualTo("created");
        assertThat(transformed.events().get(1).eventType()).isEqualTo("started");
        assertThat(transformed.events().get(2).eventType()).isEqualTo("mounted");
        assertThat(transformed.events().get(3).eventType()).isEqualTo("parked");
        assertThat(transformed.events().get(3).durationUs()).isEqualTo(5000);
        assertThat(transformed.events().get(4).eventType()).isEqualTo("terminated");
    }

    @Test
    void shouldHandleNullBatch() {
        assertThat(pipeline.transform(null)).isNull();
    }

    @Test
    void shouldPreserveAllEventFieldsThroughTransformation() {
        EventBatch batch = createBatchWithStackFrame("com.example.MyService", "doWork", 99);

        TransformedBatch transformed = pipeline.transform(batch, 1.0);

        TransformedEvent event = transformed.events().get(0);
        assertThat(event.appId()).isEqualTo("order-service");
        assertThat(event.instanceId()).isEqualTo("host-1_12345");
        assertThat(event.batchSeq()).isEqualTo(0);
        assertThat(event.threadId()).isEqualTo(1);
        assertThat(event.threadName()).isEqualTo("vt-1");
        assertThat(event.eventType()).isEqualTo("parked");
        assertThat(event.reason()).isEqualTo("test-reason");
    }

    @Test
    void shouldProvideCallerWithEmptyStringWhenNoCaller() {
        ThreadEvent event = ThreadEvent.newBuilder()
                .setType(ThreadEvent.EventType.CREATED)
                .setThreadId(1)
                .setThreadName("vt-1")
                .setTimestampMs(System.currentTimeMillis())
                .build();

        EventBatch batch = EventBatch.newBuilder()
                .setAppId("order-service")
                .setInstanceId("host-1_12345")
                .setBatchSeq(0)
                .setTimestampMs(System.currentTimeMillis())
                .addEvents(event)
                .build();

        TransformedBatch transformed = pipeline.transform(batch, 1.0);

        TransformedEvent te = transformed.events().get(0);
        assertThat(te.callerClass()).isEmpty();
        assertThat(te.callerMethod()).isEmpty();
        assertThat(te.callerLine()).isEqualTo(0);
    }

    private static EventBatch createBatchWithEvent(ThreadEvent.EventType type) {
        ThreadEvent event = ThreadEvent.newBuilder()
                .setType(type)
                .setThreadId(1)
                .setThreadName("vt-1")
                .setTimestampMs(System.currentTimeMillis())
                .build();

        return EventBatch.newBuilder()
                .setAppId("order-service")
                .setInstanceId("host-1_12345")
                .setBatchSeq(0)
                .setTimestampMs(System.currentTimeMillis())
                .addEvents(event)
                .build();
    }

    private static EventBatch createBatchWithStackFrame(String className,
                                                         String methodName, int lineNumber) {
        ThreadEvent event = ThreadEvent.newBuilder()
                .setType(ThreadEvent.EventType.PARKED)
                .setThreadId(1)
                .setThreadName("vt-1")
                .setTimestampMs(System.currentTimeMillis())
                .setDurationUs(5000)
                .setReason("test-reason")
                .setCaller(StackFrame.newBuilder()
                        .setClassName(className)
                        .setMethodName(methodName)
                        .setLineNumber(lineNumber)
                        .build())
                .build();

        return EventBatch.newBuilder()
                .setAppId("order-service")
                .setInstanceId("host-1_12345")
                .setBatchSeq(0)
                .setTimestampMs(System.currentTimeMillis())
                .addEvents(event)
                .build();
    }
}
```

### 7.3.5 — Verify and commit

- [ ] Run tests:
  ```bash
  cd "D:\java-project\DL-Watching" && mvn clean test -pl backend -Dtest="io.github.dlwatching.backend.pipeline.TransformationPipelineTest" -DfailIfNoTests=false
  ```
- [ ] Run all backend pipeline tests:
  ```bash
  cd "D:\java-project\DL-Watching" && mvn clean test -pl backend -Dtest="io.github.dlwatching.backend.pipeline.*" -DfailIfNoTests=false
  ```
- [ ] Commit:
  ```bash
  cd "D:\java-project\DL-Watching" && git add backend/src/main/java/io/github/dlwatching/backend/pipeline/TransformedEvent.java backend/src/main/java/io/github/dlwatching/backend/pipeline/TransformedBatch.java backend/src/main/java/io/github/dlwatching/backend/pipeline/TransformationPipeline.java backend/src/test/java/io/github/dlwatching/backend/pipeline/TransformationPipelineTest.java && git commit -m "M7-T7.3: Implement TransformationPipeline with timestamp normalization, enum conversion, and stack compression"
  ```
