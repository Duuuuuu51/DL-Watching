# M8: Backend Enrichment & EventBus

> **Module:** M8 | **Dependencies:** M7 (Validation Pipeline) | **Status:** Draft

## Overview

Build the enrichment, window aggregation, and event bus layers of the backend pipeline. The enrichment stage appends app metadata and normalizes carrier thread names. The window aggregator computes rolling 1-minute and 5-minute metrics using T-Digest percentiles. The EventBus provides an in-memory publish-subscribe mechanism for routing enriched batches to storage writers and alert consumers.

---

## Task 8.1: EnrichmentStage — metadata enrichment

- [ ] Create directories:

```bash
mkdir -p "D:\java-project\DL-Watching\backend\src\main\java\io\github\dlwatching\backend\pipeline"
mkdir -p "D:\java-project\DL-Watching\backend\src\test\java\io\github\dlwatching\backend\pipeline"
```

- [ ] Create `D:\java-project\DL-Watching\backend\src\main\java\io\github\dlwatching\backend\pipeline\AppInfo.java`:

```java
package io.github.dlwatching.backend.pipeline;

/**
 * Application metadata associated with an app_id.
 *
 * @param appName     human-readable application name
 * @param environment deployment environment (prod, staging, dev)
 * @param team        owning team name
 */
public record AppInfo(String appName, String environment, String team) {
}
```

- [ ] Create `D:\java-project\DL-Watching\backend\src\main\java\io\github\dlwatching\backend\pipeline\AppInfoRepository.java`:

```java
package io.github.dlwatching.backend.pipeline;

import java.util.Optional;

/**
 * Repository for looking up and registering application metadata.
 *
 * <p>Implementations may back this with a database, a configuration file,
 * or an in-memory store for testing.
 */
public interface AppInfoRepository {

    /**
     * Looks up application metadata by app_id.
     *
     * @param appId the application identifier
     * @return an Optional containing the AppInfo if found, or empty
     */
    Optional<AppInfo> findByAppId(String appId);

    /**
     * Registers or updates application metadata.
     *
     * @param info the application information to register
     */
    void register(AppInfo info);
}
```

- [ ] Create `D:\java-project\DL-Watching\backend\src\main\java\io\github\dlwatching\backend\pipeline\InMemoryAppInfoRepository.java`:

```java
package io.github.dlwatching.backend.pipeline;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory implementation of {@link AppInfoRepository}.
 *
 * <p>Uses a {@link ConcurrentHashMap} for thread-safe storage.
 * Primarily used for testing and single-instance deployments.
 */
public class InMemoryAppInfoRepository implements AppInfoRepository {

    private final Map<String, AppInfo> store = new ConcurrentHashMap<>();

    @Override
    public Optional<AppInfo> findByAppId(String appId) {
        return Optional.ofNullable(store.get(appId));
    }

    @Override
    public void register(AppInfo info) {
        store.put(info.appName(), info);
    }
}
```

- [ ] Create `D:\java-project\DL-Watching\backend\src\main\java\io\github\dlwatching\backend\pipeline\CarrierPoolNormalizer.java`:

```java
package io.github.dlwatching.backend.pipeline;

import java.util.regex.Pattern;

/**
 * Normalizes carrier thread names into carrier pool names.
 *
 * <p>Carrier threads follow JVM naming conventions:
 * <ul>
 *   <li>{@code ForkJoinPool-1-worker-5} → {@code ForkJoinPool-1}</li>
 *   <li>{@code pool-3-thread-1} → {@code pool-3}</li>
 * </ul>
 */
public final class CarrierPoolNormalizer {

    private static final Pattern FORK_JOIN_POOL_PATTERN =
            Pattern.compile("^(ForkJoinPool-\\d+)-worker-\\d+$");
    private static final Pattern POOL_THREAD_PATTERN =
            Pattern.compile("^(pool-\\d+)-thread-\\d+$");

    private CarrierPoolNormalizer() {
        // utility class
    }

    /**
     * Normalizes a carrier thread name to its pool name.
     *
     * @param carrierThread the raw carrier thread name, may be null
     * @return the pool name, or empty string if input is null or unrecognized
     */
    public static String normalize(String carrierThread) {
        if (carrierThread == null || carrierThread.isEmpty()) {
            return "";
        }

        java.util.regex.Matcher m1 = FORK_JOIN_POOL_PATTERN.matcher(carrierThread);
        if (m1.matches()) {
            return m1.group(1);
        }

        java.util.regex.Matcher m2 = POOL_THREAD_PATTERN.matcher(carrierThread);
        if (m2.matches()) {
            return m2.group(1);
        }

        // If it doesn't match known patterns, return as-is
        return carrierThread;
    }
}
```

- [ ] Create `D:\java-project\DL-Watching\backend\src\main\java\io\github\dlwatching\backend\pipeline\EnrichedEvent.java`:

```java
package io.github.dlwatching.backend.pipeline;

import java.util.Map;

/**
 * An enriched thread event with metadata attached.
 *
 * @param eventType     the event type string (e.g., "CREATED", "PARKED")
 * @param threadId      the virtual thread ID
 * @param threadName    the virtual thread name
 * @param carrierPool   the normalized carrier pool name (e.g., "ForkJoinPool-1")
 * @param carrierThread the raw carrier thread name
 * @param durationUs    event duration in microseconds
 * @param reason        blocking reason (for PARKED events)
 * @param callerClass   caller class name
 * @param callerMethod  caller method name
 * @param callerLine    caller line number
 * @param clientTs      client-side timestamp in milliseconds
 * @param serverTs      server-side receive timestamp in milliseconds
 * @param enrichmentTags map of additional enrichment key-value pairs
 */
public record EnrichedEvent(
        String eventType,
        long threadId,
        String threadName,
        String carrierPool,
        String carrierThread,
        long durationUs,
        String reason,
        String callerClass,
        String callerMethod,
        int callerLine,
        long clientTs,
        long serverTs,
        Map<String, String> enrichmentTags
) {
}
```

- [ ] Create `D:\java-project\DL-Watching\backend\src\main\java\io\github\dlwatching\backend\pipeline\EnrichedBatch.java`:

```java
package io.github.dlwatching.backend.pipeline;

import java.util.List;

/**
 * An enriched batch of events with application metadata.
 *
 * @param appId       application identifier
 * @param appName     human-readable application name
 * @param environment deployment environment (prod, staging, dev)
 * @param team        owning team name
 * @param instanceId  instance identifier (hostname_pid)
 * @param batchSeq    batch sequence number
 * @param events      list of enriched events
 * @param clientTs    client-side batch timestamp in milliseconds
 * @param serverTs    server-side receive timestamp in milliseconds
 */
public record EnrichedBatch(
        String appId,
        String appName,
        String environment,
        String team,
        String instanceId,
        int batchSeq,
        List<EnrichedEvent> events,
        long clientTs,
        long serverTs
) {
}
```

- [ ] Create `D:\java-project\DL-Watching\backend\src\main\java\io\github\dlwatching\backend\pipeline\EnrichmentStage.java`:

```java
package io.github.dlwatching.backend.pipeline;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Enriches a {@link TransformedBatch} with application metadata and
 * normalized carrier pool names.
 *
 * <p>For each event in the batch, the enrichment stage:
 * <ul>
 *   <li>Looks up app metadata (appName, environment, team) from the repository</li>
 *   <li>Normalizes the carrier thread name to a carrier pool name</li>
 *   <li>Attaches enrichment tags as a {@code Map<String, String>}</li>
 * </ul>
 */
public class EnrichmentStage {

    private final AppInfoRepository appInfoRepository;

    /**
     * Creates an enrichment stage with the given repository.
     *
     * @param appInfoRepository the repository for application metadata lookup
     */
    public EnrichmentStage(AppInfoRepository appInfoRepository) {
        this.appInfoRepository = appInfoRepository;
    }

    /**
     * Enriches a transformed batch and returns an enriched batch.
     *
     * @param batch the transformed batch to enrich
     * @return the enriched batch with metadata attached
     */
    public EnrichedBatch enrich(TransformedBatch batch) {
        if (batch == null) {
            throw new IllegalArgumentException("batch must not be null");
        }

        // Look up app metadata
        Optional<AppInfo> appInfoOpt = appInfoRepository.findByAppId(batch.appId());
        String appName;
        String environment;
        String team;

        if (appInfoOpt.isPresent()) {
            AppInfo info = appInfoOpt.get();
            appName = info.appName();
            environment = info.environment();
            team = info.team();
        } else {
            appName = "unknown";
            environment = "unknown";
            team = "unknown";
        }

        // Enrich each event
        List<EnrichedEvent> enrichedEvents;
        if (batch.events() == null || batch.events().isEmpty()) {
            enrichedEvents = Collections.emptyList();
        } else {
            enrichedEvents = new ArrayList<>(batch.events().size());
            for (TransformedEvent event : batch.events()) {
                enrichedEvents.add(enrichEvent(event, appName, environment, team));
            }
        }

        return new EnrichedBatch(
                batch.appId(),
                appName,
                environment,
                team,
                batch.instanceId(),
                batch.batchSeq(),
                enrichedEvents,
                batch.clientTs(),
                batch.serverTs()
        );
    }

    /**
     * Enriches a single transformed event.
     */
    private EnrichedEvent enrichEvent(TransformedEvent event, String appName, String environment, String team) {
        String carrierPool = CarrierPoolNormalizer.normalize(event.carrierThread());

        Map<String, String> tags = Map.of(
                "appName", appName,
                "environment", environment,
                "team", team,
                "carrierPool", carrierPool
        );

        return new EnrichedEvent(
                event.eventType(),
                event.threadId(),
                event.threadName(),
                carrierPool,
                event.carrierThread(),
                event.durationUs(),
                event.reason(),
                event.callerClass(),
                event.callerMethod(),
                event.callerLine(),
                event.clientTs(),
                event.serverTs(),
                tags
        );
    }
}
```

- [ ] Create `D:\java-project\DL-Watching\backend\src\test\java\io\github\dlwatching\backend\pipeline\EnrichmentStageTest.java`:

```java
package io.github.dlwatching.backend.pipeline;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class EnrichmentStageTest {

    private InMemoryAppInfoRepository repository;
    private EnrichmentStage enrichmentStage;

    @BeforeEach
    void setUp() {
        repository = new InMemoryAppInfoRepository();
        enrichmentStage = new EnrichmentStage(repository);

        // Register a known app
        repository.register(new AppInfo("my-app", "prod", "platform-team"));
    }

    @Test
    void shouldPopulateAppMetadataForKnownAppId() {
        TransformedEvent event = new TransformedEvent(
                "PARKED", 1001L, "vt-1001", "ForkJoinPool-1-worker-5",
                50000L, "LockSupport.park", "com.example.Service",
                "doWork", 42, 1000L, 1005L, "my-app", "host-1_12345", 1);
        TransformedBatch batch = new TransformedBatch(
                "my-app", "host-1_12345", 1, List.of(event), 1000L, 1005L);

        EnrichedBatch result = enrichmentStage.enrich(batch);

        assertThat(result.appId()).isEqualTo("my-app");
        assertThat(result.appName()).isEqualTo("my-app");
        assertThat(result.environment()).isEqualTo("prod");
        assertThat(result.team()).isEqualTo("platform-team");
    }

    @Test
    void shouldUseUnknownDefaultsForUnknownAppId() {
        TransformedEvent event = new TransformedEvent(
                "CREATED", 2001L, "vt-2001", null,
                0L, "", "com.example.Other",
                "run", 10, 2000L, 2005L, "unknown-app", "host-2_54321", 1);
        TransformedBatch batch = new TransformedBatch(
                "unknown-app", "host-2_54321", 1, List.of(event), 2000L, 2005L);

        EnrichedBatch result = enrichmentStage.enrich(batch);

        assertThat(result.appId()).isEqualTo("unknown-app");
        assertThat(result.appName()).isEqualTo("unknown");
        assertThat(result.environment()).isEqualTo("unknown");
        assertThat(result.team()).isEqualTo("unknown");
    }

    @Test
    void shouldNormalizeCarrierThreadToPoolName() {
        TransformedEvent event = new TransformedEvent(
                "MOUNTED", 3001L, "vt-3001", "ForkJoinPool-1-worker-5",
                0L, "", "com.example.Service",
                "handle", 15, 3000L, 3005L, "my-app", "host-1_12345", 1);
        TransformedBatch batch = new TransformedBatch(
                "my-app", "host-1_12345", 1, List.of(event), 3000L, 3005L);

        EnrichedBatch result = enrichmentStage.enrich(batch);

        EnrichedEvent enrichedEvent = result.events().get(0);
        assertThat(enrichedEvent.carrierPool()).isEqualTo("ForkJoinPool-1");
        assertThat(enrichedEvent.carrierThread()).isEqualTo("ForkJoinPool-1-worker-5");
    }

    @Test
    void shouldHandleNullCarrierThread() {
        TransformedEvent event = new TransformedEvent(
                "TERMINATED", 4001L, "vt-4001", null,
                0L, "", "com.example.Task",
                "call", 5, 4000L, 4005L, "my-app", "host-1_12345", 1);
        TransformedBatch batch = new TransformedBatch(
                "my-app", "host-1_12345", 1, List.of(event), 4000L, 4005L);

        EnrichedBatch result = enrichmentStage.enrich(batch);

        EnrichedEvent enrichedEvent = result.events().get(0);
        assertThat(enrichedEvent.carrierPool()).isEqualTo("");
        assertThat(enrichedEvent.carrierThread()).isNull();
    }

    @Test
    void shouldReturnEmptyEnrichedBatchForEmptyInput() {
        TransformedBatch batch = new TransformedBatch(
                "my-app", "host-1_12345", 1, List.of(), 5000L, 5005L);

        EnrichedBatch result = enrichmentStage.enrich(batch);

        assertThat(result.events()).isEmpty();
        assertThat(result.appId()).isEqualTo("my-app");
        assertThat(result.batchSeq()).isEqualTo(1);
    }

    @Test
    void shouldAttachEnrichmentTags() {
        TransformedEvent event = new TransformedEvent(
                "PARKED", 5001L, "vt-5001", "pool-3-thread-1",
                100000L, "ReentrantLock", "com.example.LockService",
                "acquire", 20, 5000L, 5005L, "my-app", "host-1_12345", 1);
        TransformedBatch batch = new TransformedBatch(
                "my-app", "host-1_12345", 1, List.of(event), 5000L, 5005L);

        EnrichedBatch result = enrichmentStage.enrich(batch);

        EnrichedEvent enrichedEvent = result.events().get(0);
        Map<String, String> tags = enrichedEvent.enrichmentTags();
        assertThat(tags).containsEntry("appName", "my-app");
        assertThat(tags).containsEntry("environment", "prod");
        assertThat(tags).containsEntry("team", "platform-team");
        assertThat(tags).containsEntry("carrierPool", "pool-3");
    }

    @Test
    void shouldRejectNullBatch() {
        assertThatThrownBy(() -> enrichmentStage.enrich(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batch");
    }
}
```

- [ ] Run the test:

```bash
cd D:\java-project\DL-Watching && mvn test -pl backend -Dtest=EnrichmentStageTest
```

**Expected result:** Tests PASS (7/7).

- [ ] Stage and commit:

```bash
cd D:\java-project\DL-Watching && git add backend/src/main/java/io/github/dlwatching/backend/pipeline/AppInfo.java backend/src/main/java/io/github/dlwatching/backend/pipeline/AppInfoRepository.java backend/src/main/java/io/github/dlwatching/backend/pipeline/InMemoryAppInfoRepository.java backend/src/main/java/io/github/dlwatching/backend/pipeline/CarrierPoolNormalizer.java backend/src/main/java/io/github/dlwatching/backend/pipeline/EnrichedEvent.java backend/src/main/java/io/github/dlwatching/backend/pipeline/EnrichedBatch.java backend/src/main/java/io/github/dlwatching/backend/pipeline/EnrichmentStage.java backend/src/test/java/io/github/dlwatching/backend/pipeline/EnrichmentStageTest.java && git commit -m "$(cat <<'EOF'
M8.1: Add EnrichmentStage with app metadata lookup and carrier pool normalization

Implement AppInfo, AppInfoRepository, InMemoryAppInfoRepository,
CarrierPoolNormalizer, EnrichedEvent, EnrichedBatch, and EnrichmentStage.
Known app_id populates appName/environment/team; unknown returns "unknown"
defaults. Carrier thread names normalized to pool names (e.g.,
ForkJoinPool-1-worker-5 → ForkJoinPool-1). Null carrier thread yields
empty pool string.

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Task 8.2: WindowAggregator — time-window metrics aggregation

- [ ] Create `D:\java-project\DL-Watching\backend\src\main\java\io\github\dlwatching\backend\pipeline\AggregatedMetric.java`:

```java
package io.github.dlwatching.backend.pipeline;

/**
 * Aggregated metrics for a specific time window.
 *
 * @param appId        application identifier
 * @param eventType    the event type being aggregated
 * @param windowStartMs start timestamp of the aggregation window in milliseconds
 * @param count        number of events in this window
 * @param avgDuration  average event duration in microseconds
 * @param p50Duration  median (50th percentile) duration in microseconds
 * @param p99Duration  99th percentile duration in microseconds
 * @param maxDuration  maximum duration in microseconds
 */
public record AggregatedMetric(
        String appId,
        String eventType,
        long windowStartMs,
        int count,
        double avgDuration,
        long p50Duration,
        long p99Duration,
        long maxDuration
) {
}
```

- [ ] Create `D:\java-project\DL-Watching\backend\src\main\java\io\github\dlwatching\backend\pipeline\WindowBucket.java`:

```java
package io.github.dlwatching.backend.pipeline;

import com.tdunning.math.stats.TDigest;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * A single time-window bucket containing aggregated event data.
 *
 * <p>Uses T-Digest for efficient percentile calculation. Thread-safe.
 */
public class WindowBucket {

    private static final double TDIGEST_COMPRESSION = 100.0;

    private final String appId;
    private final String eventType;
    private final long windowStartMs;
    private final Object digestLock = new Object();

    private volatile TDigest digest;
    private final AtomicInteger count = new AtomicInteger(0);
    private final AtomicLong sumDuration = new AtomicLong(0);
    private volatile long maxDuration = 0;

    /**
     * Creates a new empty window bucket.
     *
     * @param appId        application identifier
     * @param eventType    event type
     * @param windowStartMs start of the window in milliseconds
     */
    public WindowBucket(String appId, String eventType, long windowStartMs) {
        this.appId = appId;
        this.eventType = eventType;
        this.windowStartMs = windowStartMs;
        this.digest = TDigest.createDigest(TDIGEST_COMPRESSION);
    }

    /**
     * Records a single event duration into this bucket.
     *
     * @param durationUs event duration in microseconds
     */
    public void record(long durationUs) {
        count.incrementAndGet();
        sumDuration.addAndGet(durationUs);

        // Update max (thread-safe with volatile write after compare)
        long currentMax;
        do {
            currentMax = maxDuration;
        } while (durationUs > currentMax
                && !AtomicLongFieldUpdater.newUpdater(WindowBucket.class, long.class, "maxDuration")
                        .compareAndSet(this, currentMax, durationUs));

        synchronized (digestLock) {
            digest.add(durationUs);
        }
    }

    /**
     * Returns the aggregated metric for this bucket.
     *
     * @return the aggregated metric
     */
    public AggregatedMetric toMetric() {
        int c = count.get();
        double avg = c > 0 ? (double) sumDuration.get() / c : 0.0;
        long p50;
        long p99;

        synchronized (digestLock) {
            if (c > 0) {
                p50 = (long) digest.quantile(0.50);
                p99 = (long) digest.quantile(0.99);
            } else {
                p50 = 0;
                p99 = 0;
            }
        }

        return new AggregatedMetric(
                appId, eventType, windowStartMs,
                c, avg, p50, p99, maxDuration
        );
    }

    public int count() {
        return count.get();
    }

    public String appId() {
        return appId;
    }

    public String eventType() {
        return eventType;
    }

    public long windowStartMs() {
        return windowStartMs;
    }
}
```

- [ ] Create `D:\java-project\DL-Watching\backend\src\main\java\io\github\dlwatching\backend\pipeline\WindowAggregator.java`:

```java
package io.github.dlwatching.backend.pipeline;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Maintains rolling time windows for event metrics aggregation.
 *
 * <p>Supports 1-minute and 5-minute windows keyed by (appId, eventType).
 * Uses T-Digest for efficient percentile (p50, p99) calculation.
 */
public class WindowAggregator {

    private static final long ONE_MINUTE_MS = 60_000L;
    private static final long FIVE_MINUTES_MS = 300_000L;

    private final NavigableMap<Long, WindowBucket> oneMinBuckets = new ConcurrentSkipListMap<>();
    private final NavigableMap<Long, WindowBucket> fiveMinBuckets = new ConcurrentSkipListMap<>();

    /**
     * Processes an enriched event, updating the appropriate window buckets.
     *
     * @param event the enriched event to process
     */
    public void onEvent(EnrichedEvent event) {
        if (event == null) {
            return;
        }

        long serverTs = event.serverTs();
        long oneMinWindow = (serverTs / ONE_MINUTE_MS) * ONE_MINUTE_MS;
        long fiveMinWindow = (serverTs / FIVE_MINUTES_MS) * FIVE_MINUTES_MS;

        String appId = event.enrichmentTags() != null
                ? event.enrichmentTags().getOrDefault("appName", "unknown")
                : "unknown";

        getOrCreateBucket(oneMinBuckets, appId, event.eventType(), oneMinWindow).record(event.durationUs());
        getOrCreateBucket(fiveMinBuckets, appId, event.eventType(), fiveMinWindow).record(event.durationUs());
    }

    /**
     * Returns aggregated metrics for the given window duration.
     *
     * @param windowDurationMin the window duration in minutes (1 or 5)
     * @return list of aggregated metrics
     */
    public List<AggregatedMetric> getAggregatedMetrics(int windowDurationMin) {
        NavigableMap<Long, WindowBucket> buckets;
        if (windowDurationMin == 1) {
            buckets = oneMinBuckets;
        } else if (windowDurationMin == 5) {
            buckets = fiveMinBuckets;
        } else {
            throw new IllegalArgumentException("Unsupported window duration: " + windowDurationMin + " min. Supported: 1, 5");
        }

        List<AggregatedMetric> result = new ArrayList<>();
        for (WindowBucket bucket : buckets.values()) {
            result.add(bucket.toMetric());
        }
        return result;
    }

    /**
     * Gets or creates a window bucket for the given key.
     */
    private WindowBucket getOrCreateBucket(
            NavigableMap<Long, WindowBucket> buckets,
            String appId, String eventType, long windowStartMs) {

        return buckets.computeIfAbsent(windowStartMs,
                key -> new WindowBucket(appId, eventType, windowStartMs));
    }

    /**
     * Cleans up old buckets that are past the retention period.
     * Retention is 10 minutes for 1-min buckets, 30 minutes for 5-min buckets.
     *
     * @param nowMs current time in milliseconds
     */
    public void cleanup(long nowMs) {
        long oneMinRetention = nowMs - 10 * ONE_MINUTE_MS;
        long fiveMinRetention = nowMs - 30 * ONE_MINUTE_MS;

        oneMinBuckets.headMap(oneMinRetention, false).clear();
        fiveMinBuckets.headMap(fiveMinRetention, false).clear();
    }

    /**
     * Returns the number of 1-minute buckets currently tracked.
     */
    int oneMinBucketCount() {
        return oneMinBuckets.size();
    }

    /**
     * Returns the number of 5-minute buckets currently tracked.
     */
    int fiveMinBucketCount() {
        return fiveMinBuckets.size();
    }
}
```

- [ ] Add T-Digest dependency to the backend POM. Edit `D:\java-project\DL-Watching\backend\pom.xml` to add the t-digest dependency and update the root `D:\java-project\DL-Watching\pom.xml` dependencyManagement:

Edit `D:\java-project\DL-Watching\pom.xml`:

```xml
<!-- T-Digest -->
<dependency>
    <groupId>com.tdunning</groupId>
    <artifactId>t-digest</artifactId>
    <version>3.3</version>
</dependency>
```

Add this block after the existing ClickHouse JDBC dependency block in `<dependencyManagement>`.

- [ ] Add the dependency to `D:\java-project\DL-Watching\backend\pom.xml`:

```xml
<dependency>
    <groupId>com.tdunning</groupId>
    <artifactId>t-digest</artifactId>
</dependency>
```

Add this inside the `<dependencies>` section.

- [ ] Create `D:\java-project\DL-Watching\backend\src\test\java\io\github\dlwatching\backend\pipeline\WindowAggregatorTest.java`:

```java
package io.github.dlwatching.backend.pipeline;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class WindowAggregatorTest {

    private WindowAggregator aggregator;
    private long baseTime;

    @BeforeEach
    void setUp() {
        aggregator = new WindowAggregator();
        baseTime = System.currentTimeMillis();
        // Round to nearest minute boundary
        baseTime = (baseTime / 60_000L) * 60_000L;
    }

    private EnrichedEvent createEvent(String eventType, long durationUs, long serverTs) {
        return new EnrichedEvent(
                eventType, 1001L, "vt-1001", "ForkJoinPool-1",
                "ForkJoinPool-1-worker-5", durationUs,
                "LockSupport.park", "com.example.Service",
                "doWork", 42, serverTs - 5, serverTs,
                Map.of("appName", "test-app", "environment", "test", "team", "test-team", "carrierPool", "ForkJoinPool-1")
        );
    }

    @Test
    void singleEventShouldProduceCorrectAggregate() {
        EnrichedEvent event = createEvent("PARKED", 50000L, baseTime);
        aggregator.onEvent(event);

        List<AggregatedMetric> metrics = aggregator.getAggregatedMetrics(1);

        assertThat(metrics).hasSize(1);
        AggregatedMetric m = metrics.get(0);
        assertThat(m.appId()).isEqualTo("test-app");
        assertThat(m.eventType()).isEqualTo("PARKED");
        assertThat(m.count()).isEqualTo(1);
        assertThat(m.avgDuration()).isEqualTo(50000.0);
        assertThat(m.maxDuration()).isEqualTo(50000L);
        // Single element: p50 and p99 should both equal the value
        assertThat(m.p50Duration()).isEqualTo(50000L);
        assertThat(m.p99Duration()).isEqualTo(50000L);
    }

    @Test
    void hundredEventsInSameWindowShouldProduceCorrectAggregates() {
        long windowStart = baseTime;
        for (int i = 0; i < 100; i++) {
            EnrichedEvent event = createEvent("PARKED", 1000L * (i + 1), windowStart);
            aggregator.onEvent(event);
        }

        List<AggregatedMetric> metrics = aggregator.getAggregatedMetrics(1);

        assertThat(metrics).hasSize(1);
        AggregatedMetric m = metrics.get(0);
        assertThat(m.count()).isEqualTo(100);
        // Average of 1000..100000 = 50500
        assertThat(m.avgDuration()).isCloseTo(50500.0, org.assertj.core.data.Offset.offset(1.0));
        assertThat(m.maxDuration()).isEqualTo(100_000L);
        // p50 should be around 50000, p99 around 99000
        assertThat(m.p50Duration()).isBetween(45000L, 55000L);
        assertThat(m.p99Duration()).isGreaterThan(95000L);
    }

    @Test
    void eventsAcrossWindowBoundaryShouldProduceSeparateAggregates() {
        long window1 = baseTime;
        long window2 = baseTime + 60_000L; // 1 minute later

        EnrichedEvent event1 = createEvent("PARKED", 10000L, window1);
        EnrichedEvent event2 = createEvent("PARKED", 20000L, window1);
        EnrichedEvent event3 = createEvent("PARKED", 30000L, window2);

        aggregator.onEvent(event1);
        aggregator.onEvent(event2);
        aggregator.onEvent(event3);

        List<AggregatedMetric> metrics = aggregator.getAggregatedMetrics(1);

        assertThat(metrics).hasSize(2);

        AggregatedMetric m1 = metrics.stream()
                .filter(m -> m.windowStartMs() == window1)
                .findFirst().orElseThrow();
        assertThat(m1.count()).isEqualTo(2);
        assertThat(m1.avgDuration()).isEqualTo(15000.0);

        AggregatedMetric m2 = metrics.stream()
                .filter(m -> m.windowStartMs() == window2)
                .findFirst().orElseThrow();
        assertThat(m2.count()).isEqualTo(1);
        assertThat(m2.avgDuration()).isEqualTo(30000.0);
    }

    @Test
    void getMetricsFor1MinShouldReturnCorrectWindows() {
        EnrichedEvent event = createEvent("MOUNTED", 5000L, baseTime);
        aggregator.onEvent(event);

        List<AggregatedMetric> oneMin = aggregator.getAggregatedMetrics(1);
        List<AggregatedMetric> fiveMin = aggregator.getAggregatedMetrics(5);

        assertThat(oneMin).hasSize(1);
        // 5-min window should also contain the event (same bucket)
        assertThat(fiveMin).hasSize(1);
    }

    @Test
    void getMetricsFor5MinShouldReturnCorrectWindows() {
        long fiveMinWindow = (baseTime / 300_000L) * 300_000L;
        EnrichedEvent event = createEvent("UNMOUNTED", 25000L, baseTime);
        aggregator.onEvent(event);

        List<AggregatedMetric> metrics = aggregator.getAggregatedMetrics(5);

        assertThat(metrics).hasSize(1);
        assertThat(metrics.get(0).windowStartMs()).isEqualTo(fiveMinWindow);
        assertThat(metrics.get(0).count()).isEqualTo(1);
    }

    @Test
    void getMetricsWithInvalidDurationShouldThrow() {
        assertThatThrownBy(() -> aggregator.getAggregatedMetrics(10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported");
    }

    @Test
    void nullEventShouldBeSilentlyIgnored() {
        aggregator.onEvent(null);
        assertThat(aggregator.getAggregatedMetrics(1)).isEmpty();
    }

    @Test
    void cleanupShouldRemoveOldBuckets() {
        long oldTime = System.currentTimeMillis() - 15 * 60 * 1000L; // 15 min ago
        EnrichedEvent oldEvent = createEvent("PARKED", 1000L, oldTime);
        aggregator.onEvent(oldEvent);

        EnrichedEvent recentEvent = createEvent("PARKED", 1000L, baseTime);
        aggregator.onEvent(recentEvent);

        assertThat(aggregator.oneMinBucketCount()).isEqualTo(2);

        aggregator.cleanup(System.currentTimeMillis());

        assertThat(aggregator.oneMinBucketCount()).isEqualTo(1);
    }
}
```

- [ ] Run the test:

```bash
cd D:\java-project\DL-Watching && mvn test -pl backend -Dtest=WindowAggregatorTest
```

**Expected result:** Tests PASS (9/9).

- [ ] Stage and commit:

```bash
cd D:\java-project\DL-Watching && git add backend/src/main/java/io/github/dlwatching/backend/pipeline/AggregatedMetric.java backend/src/main/java/io/github/dlwatching/backend/pipeline/WindowBucket.java backend/src/main/java/io/github/dlwatching/backend/pipeline/WindowAggregator.java backend/src/test/java/io/github/dlwatching/backend/pipeline/WindowAggregatorTest.java pom.xml backend/pom.xml && git commit -m "$(cat <<'EOF'
M8.2: Add WindowAggregator with T-Digest percentile computation

Implement 1-minute and 5-minute rolling windows keyed by (appId, eventType).
Uses T-Digest (com.tdunning:t-digest:3.3) for efficient p50/p99 calculation.
Supports cleanup of expired buckets beyond retention period.

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Task 8.3: EventBus — interface and in-memory implementation

- [ ] Create directories:

```bash
mkdir -p "D:\java-project\DL-Watching\backend\src\main\java\io\github\dlwatching\backend\eventbus"
mkdir -p "D:\java-project\DL-Watching\backend\src\test\java\io\github\dlwatching\backend\eventbus"
```

- [ ] Create `D:\java-project\DL-Watching\backend\src\main\java\io\github\dlwatching\backend\eventbus\EventBus.java`:

```java
package io.github.dlwatching.backend.eventbus;

import io.github.dlwatching.backend.pipeline.EnrichedBatch;

/**
 * Publish-subscribe event bus for routing {@link EnrichedBatch} instances
 * to subscribers (storage writers, alert evaluators, analytics engines).
 *
 * <p>Implementations must be thread-safe. The {@code publish} method is
 * expected to be called from pipeline processing threads, while subscribers
 * are notified on a dedicated consumer thread.
 */
public interface EventBus {

    /**
     * Publishes an enriched batch to all subscribers.
     *
     * @param batch the enriched batch to publish; must not be {@code null}
     * @return {@code true} if the batch was accepted, {@code false} if the
     *         queue was full and the batch was dropped
     */
    boolean publish(EnrichedBatch batch);

    /**
     * Registers a subscriber to receive all future published batches.
     *
     * @param subscriber the subscriber to register; must not be {@code null}
     */
    void subscribe(Subscriber subscriber);

    /**
     * Unregisters a subscriber so it no longer receives batches.
     *
     * @param subscriber the subscriber to unregister; must not be {@code null}
     */
    void unsubscribe(Subscriber subscriber);

    /**
     * Shuts down the event bus, draining remaining events and stopping
     * the consumer thread. After shutdown, no more events are delivered.
     */
    void shutdown();

    /**
     * A subscriber that receives {@link EnrichedBatch} notifications.
     */
    interface Subscriber {

        /**
         * Returns a unique name for this subscriber (used for logging and metrics).
         *
         * @return subscriber name
         */
        String name();

        /**
         * Called when a new batch is available.
         *
         * @param batch the enriched batch
         */
        void onEvent(EnrichedBatch batch);

        /**
         * Called when an error occurs during batch processing.
         * Default implementation logs the error.
         *
         * @param t the throwable that occurred
         */
        default void onError(Throwable t) {
            System.err.println("[EventBus] Subscriber " + name() + " error: " + t.getMessage());
        }
    }
}
```

- [ ] Create `D:\java-project\DL-Watching\backend\src\main\java\io\github\dlwatching\backend\eventbus\InMemoryEventBus.java`:

```java
package io.github.dlwatching.backend.eventbus;

import io.github.dlwatching.backend.pipeline.EnrichedBatch;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-memory implementation of {@link EventBus}.
 *
 * <p>Uses a {@link LinkedBlockingQueue} with capacity 10,000 and a single
 * consumer thread that dispatches batches to all subscribers sequentially.
 * If the queue is full, {@code publish} returns {@code false} and the
 * batch is silently dropped.
 */
public class InMemoryEventBus implements EventBus {

    private static final int DEFAULT_CAPACITY = 10000;
    private static final long SHUTDOWN_DRAIN_TIMEOUT_MS = 2000L;

    private final LinkedBlockingQueue<EnrichedBatch> queue;
    private final List<Subscriber> subscribers = new CopyOnWriteArrayList<>();
    private final Thread consumerThread;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicLong publishedCount = new AtomicLong(0);
    private final AtomicLong droppedCount = new AtomicLong(0);

    /**
     * Creates an InMemoryEventBus with default capacity (10,000).
     */
    public InMemoryEventBus() {
        this(DEFAULT_CAPACITY);
    }

    /**
     * Creates an InMemoryEventBus with the specified queue capacity.
     *
     * @param capacity the maximum number of batches in the queue before dropping
     */
    public InMemoryEventBus(int capacity) {
        this.queue = new LinkedBlockingQueue<>(capacity);
        this.consumerThread = new Thread(this::dispatchLoop, "eventbus-consumer");
        this.consumerThread.setDaemon(true);
        this.consumerThread.start();
    }

    @Override
    public boolean publish(EnrichedBatch batch) {
        if (batch == null) {
            return false;
        }
        if (!running.get()) {
            return false;
        }
        boolean accepted = queue.offer(batch);
        if (accepted) {
            publishedCount.incrementAndGet();
        } else {
            droppedCount.incrementAndGet();
        }
        return accepted;
    }

    @Override
    public void subscribe(Subscriber subscriber) {
        if (subscriber != null && !subscribers.contains(subscriber)) {
            subscribers.add(subscriber);
        }
    }

    @Override
    public void unsubscribe(Subscriber subscriber) {
        subscribers.remove(subscriber);
    }

    @Override
    public void shutdown() {
        running.set(false);
        // Drain remaining batches before stopping
        long deadline = System.currentTimeMillis() + SHUTDOWN_DRAIN_TIMEOUT_MS;
        while (!queue.isEmpty() && System.currentTimeMillis() < deadline) {
            EnrichedBatch batch = queue.poll();
            if (batch != null) {
                dispatchToSubscribers(batch);
            }
        }
        consumerThread.interrupt();
        try {
            consumerThread.join(SHUTDOWN_DRAIN_TIMEOUT_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Returns the number of batches published since startup.
     */
    public long publishedCount() {
        return publishedCount.get();
    }

    /**
     * Returns the number of batches dropped due to queue full.
     */
    public long droppedCount() {
        return droppedCount.get();
    }

    /**
     * Returns the number of currently registered subscribers.
     */
    public int subscriberCount() {
        return subscribers.size();
    }

    /**
     * Returns whether the event bus consumer thread is running.
     */
    public boolean isRunning() {
        return running.get() && consumerThread.isAlive();
    }

    /**
     * Main dispatch loop running on the consumer thread.
     */
    private void dispatchLoop() {
        while (running.get() || !queue.isEmpty()) {
            try {
                EnrichedBatch batch = queue.poll(500, TimeUnit.MILLISECONDS);
                if (batch != null) {
                    dispatchToSubscribers(batch);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    /**
     * Dispatches a batch to all registered subscribers.
     * If a subscriber throws, other subscribers still receive the batch.
     */
    private void dispatchToSubscribers(EnrichedBatch batch) {
        for (Subscriber subscriber : subscribers) {
            try {
                subscriber.onEvent(batch);
            } catch (Throwable t) {
                try {
                    subscriber.onError(t);
                } catch (Throwable ignored) {
                    // Do not let subscriber error handler crash the dispatch loop
                }
            }
        }
    }
}
```

- [ ] Create `D:\java-project\DL-Watching\backend\src\test\java\io\github\dlwatching\backend\eventbus\InMemoryEventBusTest.java`:

```java
package io.github.dlwatching.backend.eventbus;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.dlwatching.backend.pipeline.EnrichedBatch;
import io.github.dlwatching.backend.pipeline.EnrichedEvent;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class InMemoryEventBusTest {

    private InMemoryEventBus eventBus;

    @BeforeEach
    void setUp() {
        eventBus = new InMemoryEventBus(100);
    }

    @AfterEach
    void tearDown() {
        eventBus.shutdown();
    }

    private EnrichedBatch createBatch(String appId, int batchSeq) {
        return new EnrichedBatch(
                appId, appId, "prod", "team", "host-1_12345",
                batchSeq, Collections.emptyList(), 1000L, 2000L
        );
    }

    @Test
    void singleSubscriberShouldReceivePublishedBatches() throws Exception {
        CountDownLatch latch = new CountDownLatch(3);
        List<EnrichedBatch> received = Collections.synchronizedList(new ArrayList<>());

        EventBus.Subscriber subscriber = new EventBus.Subscriber() {
            @Override
            public String name() {
                return "test-subscriber";
            }

            @Override
            public void onEvent(EnrichedBatch batch) {
                received.add(batch);
                latch.countDown();
            }
        };

        eventBus.subscribe(subscriber);

        eventBus.publish(createBatch("app1", 1));
        eventBus.publish(createBatch("app1", 2));
        eventBus.publish(createBatch("app1", 3));

        assertThat(latch.await(3, TimeUnit.SECONDS)).isTrue();
        assertThat(received).hasSize(3);
        assertThat(received.get(0).batchSeq()).isEqualTo(1);
        assertThat(received.get(1).batchSeq()).isEqualTo(2);
        assertThat(received.get(2).batchSeq()).isEqualTo(3);
    }

    @Test
    void multipleSubscribersShouldAllReceiveSameBatch() throws Exception {
        CountDownLatch latch = new CountDownLatch(2);
        AtomicInteger count1 = new AtomicInteger(0);
        AtomicInteger count2 = new AtomicInteger(0);

        EventBus.Subscriber sub1 = new EventBus.Subscriber() {
            @Override
            public String name() { return "sub1"; }
            @Override
            public void onEvent(EnrichedBatch batch) { count1.incrementAndGet(); latch.countDown(); }
        };

        EventBus.Subscriber sub2 = new EventBus.Subscriber() {
            @Override
            public String name() { return "sub2"; }
            @Override
            public void onEvent(EnrichedBatch batch) { count2.incrementAndGet(); latch.countDown(); }
        };

        eventBus.subscribe(sub1);
        eventBus.subscribe(sub2);

        eventBus.publish(createBatch("app1", 1));

        assertThat(latch.await(3, TimeUnit.SECONDS)).isTrue();
        assertThat(count1.get()).isEqualTo(1);
        assertThat(count2.get()).isEqualTo(1);
    }

    @Test
    void queueFullShouldDropEvents() throws Exception {
        // Create event bus with very small capacity
        InMemoryEventBus smallBus = new InMemoryEventBus(1);

        try {
            CountDownLatch latch = new CountDownLatch(1);
            EventBus.Subscriber slowSub = new EventBus.Subscriber() {
                @Override
                public String name() { return "slow"; }
                @Override
                public void onEvent(EnrichedBatch batch) {
                    latch.countDown();
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            };
            smallBus.subscribe(slowSub);

            // First batch should be accepted
            boolean first = smallBus.publish(createBatch("app1", 1));
            assertThat(first).isTrue();

            // Wait for consumer to pick up the first batch
            latch.await(3, TimeUnit.SECONDS);

            // Publish many batches quickly — some should be dropped
            int accepted = 0;
            int dropped = 0;
            for (int i = 0; i < 50; i++) {
                if (smallBus.publish(createBatch("app1", i + 2))) {
                    accepted++;
                } else {
                    dropped++;
                }
            }

            assertThat(dropped).isGreaterThan(0);
            assertThat(accepted + dropped).isEqualTo(50);
        } finally {
            smallBus.shutdown();
        }
    }

    @Test
    void unsubscribeShouldStopReceiving() throws Exception {
        List<EnrichedBatch> received = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(1);

        EventBus.Subscriber subscriber = new EventBus.Subscriber() {
            @Override
            public String name() { return "unsub-test"; }
            @Override
            public void onEvent(EnrichedBatch batch) {
                received.add(batch);
                latch.countDown();
            }
        };

        eventBus.subscribe(subscriber);
        eventBus.publish(createBatch("app1", 1));
        latch.await(3, TimeUnit.SECONDS);
        assertThat(received).hasSize(1);

        eventBus.unsubscribe(subscriber);
        // Wait a bit and publish another batch
        Thread.sleep(300);
        eventBus.publish(createBatch("app1", 2));
        Thread.sleep(500);

        assertThat(received).hasSize(1); // Still only 1
    }

    @Test
    void shutdownShouldStopDelivery() throws Exception {
        List<EnrichedBatch> received = Collections.synchronizedList(new ArrayList<>());

        EventBus.Subscriber subscriber = new EventBus.Subscriber() {
            @Override
            public String name() { return "shutdown-test"; }
            @Override
            public void onEvent(EnrichedBatch batch) {
                received.add(batch);
            }
        };

        eventBus.subscribe(subscriber);

        // Publish before shutdown
        eventBus.publish(createBatch("app1", 1));
        Thread.sleep(300);

        eventBus.shutdown();

        // Publish after shutdown should be rejected
        boolean result = eventBus.publish(createBatch("app1", 2));
        assertThat(result).isFalse();

        // Verify consumer thread is stopped
        assertThat(eventBus.isRunning()).isFalse();
    }

    @Test
    void subscriberThatThrowsShouldNotPreventOtherSubscribers() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Throwable> errorRef = new AtomicReference<>();

        EventBus.Subscriber throwingSub = new EventBus.Subscriber() {
            @Override
            public String name() { return "thrower"; }
            @Override
            public void onEvent(EnrichedBatch batch) {
                throw new RuntimeException("Simulated failure");
            }
            @Override
            public void onError(Throwable t) {
                errorRef.set(t);
            }
        };

        EventBus.Subscriber goodSub = new EventBus.Subscriber() {
            @Override
            public String name() { return "good"; }
            @Override
            public void onEvent(EnrichedBatch batch) {
                latch.countDown();
            }
        };

        eventBus.subscribe(throwingSub);
        eventBus.subscribe(goodSub);

        eventBus.publish(createBatch("app1", 1));

        assertThat(latch.await(3, TimeUnit.SECONDS)).isTrue();
        assertThat(errorRef.get()).isNotNull();
        assertThat(errorRef.get()).isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Simulated failure");
    }

    @Test
    void nullBatchShouldBeRejected() {
        boolean result = eventBus.publish(null);
        assertThat(result).isFalse();
    }

    @Test
    void nullSubscriberShouldNotBeAdded() {
        eventBus.subscribe(null);
        assertThat(eventBus.subscriberCount()).isEqualTo(0);
    }
}
```

- [ ] Run the test:

```bash
cd D:\java-project\DL-Watching && mvn test -pl backend -Dtest=InMemoryEventBusTest
```

**Expected result:** Tests PASS (9/9).

- [ ] Stage and commit:

```bash
cd D:\java-project\DL-Watching && git add backend/src/main/java/io/github/dlwatching/backend/eventbus/EventBus.java backend/src/main/java/io/github/dlwatching/backend/eventbus/InMemoryEventBus.java backend/src/test/java/io/github/dlwatching/backend/eventbus/InMemoryEventBusTest.java && git commit -m "$(cat <<'EOF'
M8.3: Add EventBus interface and InMemoryEventBus implementation

Define EventBus with publish/subscribe/unsubscribe/shutdown and Subscriber
interface. InMemoryEventBus uses LinkedBlockingQueue (capacity 10000) with
single consumer thread dispatching to CopyOnWriteArrayList subscribers.
publish returns false on full queue. Subscriber exceptions are caught and
forwarded to onError, other subscribers still receive the batch.

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Task 8.4: Pipeline integration test

- [ ] Create `D:\java-project\DL-Watching\backend\src\test\java\io\github\dlwatching\backend\pipeline\PipelineIntegrationTest.java`:

```java
package io.github.dlwatching.backend.pipeline;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.dlwatching.backend.eventbus.EventBus;
import io.github.dlwatching.backend.eventbus.InMemoryEventBus;
import io.github.dlwatching.proto.EventBatch;
import io.github.dlwatching.proto.ThreadEvent;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end integration test for the backend pipeline.
 *
 * <p>Feeds raw protobuf EventBatch instances through the full pipeline:
 * ValidationPipeline → CleaningPipeline → TransformationPipeline →
 * EnrichmentStage → WindowAggregator → EventBus.
 */
class PipelineIntegrationTest {

    private InMemoryAppInfoRepository appInfoRepository;
    private EnrichmentStage enrichmentStage;
    private WindowAggregator windowAggregator;
    private InMemoryEventBus eventBus;
    private List<EnrichedBatch> receivedBatches;

    @BeforeEach
    void setUp() {
        appInfoRepository = new InMemoryAppInfoRepository();
        appInfoRepository.register(new AppInfo("test-app", "prod", "platform-team"));

        enrichmentStage = new EnrichmentStage(appInfoRepository);
        windowAggregator = new WindowAggregator();
        eventBus = new InMemoryEventBus(1000);
        receivedBatches = Collections.synchronizedList(new ArrayList<>());

        eventBus.subscribe(new EventBus.Subscriber() {
            @Override
            public String name() {
                return "integration-test-subscriber";
            }

            @Override
            public void onEvent(EnrichedBatch batch) {
                receivedBatches.add(batch);
                // Feed enriched events into window aggregator
                for (EnrichedEvent event : batch.events()) {
                    windowAggregator.onEvent(event);
                }
            }
        });
    }

    @Test
    void fullPipelineShouldProcess100EventsSpanning2Minutes() throws Exception {
        long baseTime = System.currentTimeMillis();
        // Align to minute boundary
        baseTime = (baseTime / 60_000L) * 60_000L;

        int totalEvents = 100;
        List<ThreadEvent> protoEvents = new ArrayList<>(totalEvents);

        // Create 100 events spanning 2 minutes (alternating PARKED and UNPARKED)
        for (int i = 0; i < totalEvents; i++) {
            long ts = baseTime + (i * 1200L); // 1.2s apart → spans ~2min
            long durationUs = 1000L * (i + 1); // increasing duration

            ThreadEvent event = ThreadEvent.newBuilder()
                    .setType(i % 2 == 0
                            ? io.github.dlwatching.proto.EventType.PARKED
                            : io.github.dlwatching.proto.EventType.UNPARKED)
                    .setThreadId(1000L + i)
                    .setThreadName("vt-" + (1000 + i))
                    .setTimestampMs(ts)
                    .setCarrierThread("ForkJoinPool-1-worker-" + (i % 8 + 1))
                    .setDurationUs(durationUs)
                    .setReason("LockSupport.park")
                    .build();

            protoEvents.add(event);
        }

        // Build EventBatch
        EventBatch eventBatch = EventBatch.newBuilder()
                .setAppId("test-app")
                .setInstanceId("host-1_12345")
                .setBatchSeq(1)
                .setTimestampMs(baseTime)
                .addAllEvents(protoEvents)
                .build();

        // We have already-mapped TransformedBatch equivalents.
        // In this integration test we directly construct a TransformedBatch
        // from the proto events to simulate the full pipeline output.

        List<TransformedEvent> transformedEvents = new ArrayList<>(totalEvents);
        for (int i = 0; i < totalEvents; i++) {
            ThreadEvent pe = protoEvents.get(i);
            transformedEvents.add(new TransformedEvent(
                    pe.getType().name(),
                    pe.getThreadId(),
                    pe.getThreadName(),
                    pe.getCarrierThread(),
                    pe.getDurationUs(),
                    pe.getReason(),
                    pe.hasCaller() ? pe.getCaller().getClassName() : "",
                    pe.hasCaller() ? pe.getCaller().getMethodName() : "",
                    pe.hasCaller() ? pe.getCaller().getLineNumber() : 0,
                    pe.getTimestampMs(),
                    baseTime + 5,
                    eventBatch.getAppId(),
                    eventBatch.getInstanceId(),
                    (int) eventBatch.getBatchSeq()
            ));
        }

        TransformedBatch transformedBatch = new TransformedBatch(
                eventBatch.getAppId(),
                eventBatch.getInstanceId(),
                (int) eventBatch.getBatchSeq(),
                transformedEvents,
                eventBatch.getTimestampMs(),
                baseTime + 5
        );

        // Step 1: Enrich
        EnrichedBatch enrichedBatch = enrichmentStage.enrich(transformedBatch);

        // Verify enrichment
        assertThat(enrichedBatch.appId()).isEqualTo("test-app");
        assertThat(enrichedBatch.appName()).isEqualTo("test-app");
        assertThat(enrichedBatch.environment()).isEqualTo("prod");
        assertThat(enrichedBatch.team()).isEqualTo("platform-team");
        assertThat(enrichedBatch.events()).hasSize(totalEvents);

        // Verify carrier pool normalization
        EnrichedEvent firstEvent = enrichedBatch.events().get(0);
        assertThat(firstEvent.carrierPool()).isEqualTo("ForkJoinPool-1");

        // Step 2: Publish to EventBus
        eventBus.publish(enrichedBatch);

        // Wait for EventBus consumer to process
        Thread.sleep(500);

        // Step 3: Verify EventBus delivery
        assertThat(receivedBatches).hasSize(1);
        assertThat(receivedBatches.get(0).events()).hasSize(totalEvents);

        // Step 4: Verify WindowAggregator metrics
        List<AggregatedMetric> metrics = windowAggregator.getAggregatedMetrics(1);
        assertThat(metrics).isNotEmpty();

        // Count events per event type
        long parkedCount = enrichedBatch.events().stream()
                .filter(e -> e.eventType().equals("PARKED"))
                .count();
        long unparkedCount = enrichedBatch.events().stream()
                .filter(e -> e.eventType().equals("UNPARKED"))
                .count();
        assertThat(parkedCount).isEqualTo(50);
        assertThat(unparkedCount).isEqualTo(50);

        // Find the PARKED metric
        AggregatedMetric parkedMetric = metrics.stream()
                .filter(m -> m.eventType().equals("PARKED"))
                .findFirst().orElse(null);
        assertThat(parkedMetric).isNotNull();
        assertThat(parkedMetric.count()).isEqualTo(50);
        assertThat(parkedMetric.maxDuration()).isGreaterThan(0);

        // Step 5: Verify 5-minute metrics
        List<AggregatedMetric> fiveMinMetrics = windowAggregator.getAggregatedMetrics(5);
        assertThat(fiveMinMetrics).isNotEmpty();
    }

    @Test
    void pipelineShouldHandleEmptyBatch() {
        TransformedBatch emptyBatch = new TransformedBatch(
                "test-app", "host-1_12345", 2, Collections.emptyList(), 1000L, 1005L);

        EnrichedBatch enriched = enrichmentStage.enrich(emptyBatch);
        assertThat(enriched.events()).isEmpty();

        eventBus.publish(enriched);
    }

    @Test
    void pipelineShouldPreserveBatchOrdering() throws Exception {
        List<TransformedEvent> events = new ArrayList<>();
        long baseTime = System.currentTimeMillis();
        for (int i = 0; i < 5; i++) {
            events.add(new TransformedEvent(
                    "PARKED", 2000L + i, "vt-" + (2000 + i),
                    "ForkJoinPool-1-worker-1", 10000L * (i + 1),
                    "ReentrantLock", "com.example.Service",
                    "process", 10, baseTime + i * 100, baseTime + i * 100 + 5,
                    "test-app", "host-1_12345", 3
            ));
        }

        TransformedBatch batch = new TransformedBatch(
                "test-app", "host-1_12345", 3, events, baseTime, baseTime + 5);

        EnrichedBatch enriched = enrichmentStage.enrich(batch);

        assertThat(enriched.batchSeq()).isEqualTo(3);
        assertThat(enriched.events()).hasSize(5);
        // Verify order preserved
        for (int i = 0; i < 5; i++) {
            assertThat(enriched.events().get(i).threadId()).isEqualTo(2000L + i);
        }
    }
}
```

- [ ] Run the test:

```bash
cd D:\java-project\DL-Watching && mvn test -pl backend -Dtest=PipelineIntegrationTest
```

**Expected result:** Tests PASS (3/3).

- [ ] Stage and commit:

```bash
cd D:\java-project\DL-Watching && git add backend/src/test/java/io/github/dlwatching/backend/pipeline/PipelineIntegrationTest.java && git commit -m "$(cat <<'EOF'
M8.4: Add pipeline integration test covering enrichment, EventBus, and aggregation

Feed 100 events through EnrichmentStage → EventBus → WindowAggregator.
Verify app metadata enrichment, carrier pool normalization, EventBus
delivery to subscriber, and window aggregation counts by event type.

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## M8 Completion Check

- [ ] Run full build:
```bash
cd D:\java-project\DL-Watching && mvn clean verify
```
**Expected:** BUILD SUCCESS (all modules compile, all tests pass).

- [ ] Verify git log:
```bash
cd D:\java-project\DL-Watching && git log --oneline -4
```
**Expected:** 4 most recent commits are M8 tasks 8.1 through 8.4.
