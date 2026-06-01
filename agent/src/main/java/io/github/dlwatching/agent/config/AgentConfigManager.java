package io.github.dlwatching.agent.config;

import io.github.dlwatching.proto.AgentConfig;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manages agent configuration lifecycle, loading from a local properties file
 * and merging remote overrides from the backend.
 *
 * <p>Remote config can override batch size, flush interval, and sample rate,
 * but max memory and max cache events are local hard caps that remote cannot
 * increase.
 *
 * @author Duuuuuu &lt;1617714380@qq.com&gt; @since 2026-06-01
 */
public class AgentConfigManager {

    private static final Logger log = Logger.getLogger(AgentConfigManager.class.getName());
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
                log.info("Loaded configuration from " + resource);
            } else {
                log.warning("Configuration resource " + resource + " not found, using built-in defaults");
            }
        } catch (IOException e) {
            log.log(Level.WARNING, "Failed to load configuration from " + resource, e);
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
     * @throws ConfigException if any required field is missing or invalid
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
     * for batch_size, flush_interval_ms, and sample_rate. Max cache events and
     * max memory are local caps that remote cannot increase.
     */
    public void applyRemoteConfig(AgentConfig remote) {
        if (remote.getBatchSize() > 0) {
            this.batchSize = remote.getBatchSize();
        }
        if (remote.getFlushIntervalMs() > 0) {
            this.flushIntervalMs = remote.getFlushIntervalMs();
        }
        double remoteRate = remote.getSampleRate();
        this.sampleRate = clamp(remoteRate, 0.0, 1.0);
        log.info("Applied remote config: batchSize=" + batchSize +
                ", flushIntervalMs=" + flushIntervalMs + ", sampleRate=" + sampleRate);
    }

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
