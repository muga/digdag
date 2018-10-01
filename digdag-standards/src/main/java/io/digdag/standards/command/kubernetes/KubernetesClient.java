package io.digdag.standards.command.kubernetes;

import io.digdag.spi.CommandContext;
import io.digdag.spi.CommandRequest;

import java.io.IOException;
import java.util.List;

public interface KubernetesClient
        extends AutoCloseable
{
    KubernetesClientConfig getConfig();

    io.fabric8.kubernetes.api.model.Pod runPod(
            CommandContext context, CommandRequest request,
            String name, List<String> commands, List<String> arguments);

    io.fabric8.kubernetes.api.model.Pod pollPod(String podName);

    boolean isContainerWaiting(io.fabric8.kubernetes.api.model.ContainerStatus containerStatus);

    String getLog(String podName, long offset) throws IOException;

    @Override
    void close();
}
