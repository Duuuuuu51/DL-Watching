package io.github.dlwatching.agent.reporter;

import io.github.dlwatching.agent.cache.BatchListener;
import io.github.dlwatching.proto.AgentConfig;
import io.github.dlwatching.proto.ControlCommand;
import io.github.dlwatching.proto.EventBatch;
import io.github.dlwatching.proto.HeartbeatRequest;
import io.github.dlwatching.proto.HeartbeatResponse;
import io.github.dlwatching.proto.RegisterRequest;
import io.github.dlwatching.proto.RegisterResponse;
import io.github.dlwatching.proto.VirtualThreadMonitorGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * gRPC streaming client that delivers {@link EventBatch} batches to the
 * backend via a bidirectional stream with automatic reconnection and retry.
 *
 * <p>Implements {@link BatchListener} so that {@link io.github.dlwatching.agent.cache.BatchAggregator}
 * can directly invoke it when a batch is ready.
 *
 * @author Duuuuuu &lt;1617714380@qq.com&gt; @since 2026-06-01
 */
public class GrpcReporter implements BatchListener {

    private static final Logger log = Logger.getLogger(GrpcReporter.class.getName());
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

    public GrpcReporter(ManagedChannel channel, String token, String appId, String instanceId) {
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
                log.warning("Dropping batch " + batch.getBatchSeq() + " due to connection failure");
                return;
            }
        }
        try {
            requestObserver.onNext(batch);
            sentBatches.incrementAndGet();
        } catch (Exception e) {
            log.log(Level.WARNING, "Failed to send batch " + batch.getBatchSeq() + ", reconnecting...", e);
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
                log.info("Successfully connected on attempt " + attempt + "/" + MAX_RETRIES);
                return;
            } catch (Exception e) {
                log.warning("Connection attempt " + attempt + "/" + MAX_RETRIES + " failed: " + e.getMessage());
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
        log.severe("All " + MAX_RETRIES + " connection attempts failed");
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
            if (config.getFlushIntervalMs() > 0) {
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
                log.log(Level.WARNING, "Report stream error: " + t.getMessage(), t);
                connected = false;
            }

            @Override
            public void onCompleted() {
                log.info("Report stream completed by server");
                connected = false;
            }
        };

        this.requestObserver = asyncStub.report(responseObserver);
    }

    private void handleControlCommand(ControlCommand command) {
        switch (command.getType()) {
            case ACK:
                log.fine("Received ACK for command " + command.getCommandId());
                break;
            case SLOW_DOWN:
                log.warning("Received SLOW_DOWN command, increasing flush interval");
                this.flushIntervalMs = Math.min(this.flushIntervalMs * 2, 60_000L);
                break;
            case UPDATE_CONFIG:
                if (command.hasNewConfig()) {
                    AgentConfig config = command.getNewConfig();
                    if (config.getFlushIntervalMs() > 0) {
                        this.flushIntervalMs = config.getFlushIntervalMs();
                    }
                    log.info("Applied remote config: flushIntervalMs=" + this.flushIntervalMs);
                }
                break;
            default:
                log.fine("Received unhandled command: " + command.getType());
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
                blockingStub.heartbeat(request);
            } catch (Exception e) {
                log.log(Level.WARNING, "Heartbeat failed: " + e.getMessage(), e);
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
                log.fine("Error completing request stream: " + e.getMessage());
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
