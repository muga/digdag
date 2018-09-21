package io.digdag.standards.command.kubernetes;

import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.core.storage.StorageManager;
import io.digdag.spi.Storage;

public class TemporalConfigStorage
{
    private static final String TEMPORAL_CONFIG_STORAGE_PARAMS_PREFIX = "agent.command_executor.kubernetes.config_storage.";

    public static TemporalConfigStorage create(final StorageManager storageManager, final Config systemConfig)
            throws ConfigException
    {
        final Storage storage = storageManager.create(systemConfig, TEMPORAL_CONFIG_STORAGE_PARAMS_PREFIX);
        return new TemporalConfigStorage(storage);
    }

    private final Storage storage;

    private TemporalConfigStorage(final Storage storage)
    {
        this.storage = storage;
    }

    // TODO temporal
    public Storage getStorage()
    {
        return storage;
    }
}
