package io.digdag.util;

import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import java.time.Duration;

public abstract class AbstractWaitOperatorFactory
{
    private static final Duration DEFAULT_MIN_POLL_INTERVAL = Duration.ofSeconds(30);
    private static final Duration DEFAULT_MAX_POLL_INTERVAL = Duration.ofDays(2);
    private static final Duration DEFAULT_POLL_INTERVAL = Duration.ofMinutes(10);

    private final String pollIntervalKey;
    private final String minPollIntervalKey;
    private final String maxPollIntervalKey;

    private final Duration minPollInterval;
    private final Duration maxPollInterval;
    private final Duration pollInterval;

    protected AbstractWaitOperatorFactory(String type, Config systemConfig)
    {
        this.pollIntervalKey = String.format("config.%s.wait.poll_interval", type);
        this.minPollIntervalKey = String.format("config.%s.wait.min_poll_interval", type);
        this.maxPollIntervalKey = String.format("config.%s.wait.max_poll_interval", type);
        this.minPollInterval = systemConfig.getOptional(minPollIntervalKey, DurationParam.class)
                .transform(DurationParam::getDuration).or(DEFAULT_MIN_POLL_INTERVAL);
        this.maxPollInterval = systemConfig.getOptional(maxPollIntervalKey, DurationParam.class)
                .transform(DurationParam::getDuration).or(DEFAULT_MAX_POLL_INTERVAL);
        if (minPollInterval.getSeconds() < 0 || minPollInterval.getSeconds() > Integer.MAX_VALUE) {
            throw new ConfigException("invalid configuration value: " + minPollIntervalKey);
        }
        if (maxPollInterval.getSeconds() < minPollInterval.getSeconds() || maxPollInterval.getSeconds() > Integer.MAX_VALUE) {
            throw new ConfigException("invalid configuration value: " + maxPollIntervalKey);
        }
        this.pollInterval = systemConfig.getOptional(pollIntervalKey, DurationParam.class)
                .transform(DurationParam::getDuration).or(max(minPollInterval, DEFAULT_POLL_INTERVAL));
        if (pollInterval.getSeconds() < minPollInterval.getSeconds() || pollInterval.getSeconds() > maxPollInterval.getSeconds()) {
            throw new ConfigException("invalid configuration value: " + pollIntervalKey);
        }
    }

    private Duration max(Duration a, Duration b)
    {
        return a.compareTo(b) < 0 ? a : b;
    }

    protected int getPollInterval(Config params)
    {
        long interval = validatePollInterval(params.get("interval", DurationParam.class, DurationParam.of(pollInterval)).getDuration().getSeconds());
        assert interval >= 0 && interval <= Integer.MAX_VALUE;
        return (int) interval;
    }

    private long validatePollInterval(long interval)
    {
        if (interval < minPollInterval.getSeconds() || interval > maxPollInterval.getSeconds()) {
            throw new ConfigException("poll interval must be at least " + Durations.formatDuration(minPollInterval) +
                    " and no greater than " + Durations.formatDuration(maxPollInterval));
        }
        return interval;
    }
}
