package io.digdag.standards.command;

import io.digdag.spi.CommandExecutorContent;
import io.digdag.spi.Storage;
import io.digdag.spi.StorageFileNotFoundException;

import java.io.IOException;
import java.io.InputStream;

public class KubernetesCommandExecutorContent
        implements CommandExecutorContent
{
    public static CommandExecutorContent create(final Storage configParamsStorage, final String key, final long len)
    {
        return new KubernetesCommandExecutorContent(configParamsStorage, key, len);
    }

    private final Storage configParamsStorage;
    private final String key;
    private final long len;

    private KubernetesCommandExecutorContent(final Storage configParamsStorage, final String key, final long len)
    {
        this.configParamsStorage = configParamsStorage;
        this.key = key;
        this.len = len;
    }

    @Override
    public long getLength()
    {
        return len;
    }

    @Override
    public InputStream newInputStream()
            throws IOException
    {
        try {
            return configParamsStorage.open(key).getContentInputStream();
        }
        catch (StorageFileNotFoundException e) {
            throw new IOException(e); // TODO handling appropriately
        }
    }
}
