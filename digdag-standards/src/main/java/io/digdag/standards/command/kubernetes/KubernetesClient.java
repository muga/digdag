package io.digdag.standards.command.kubernetes;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface KubernetesClient
        extends AutoCloseable
{
    KubernetesClientConfig getConfig();

    io.fabric8.kubernetes.api.model.Pod runPod(String podName,
            Map<String, String> podLabels,
            io.fabric8.kubernetes.api.model.PodSpec podSpec);

    io.fabric8.kubernetes.api.model.Pod getPod(String podName);

    boolean isContainerWaiting(io.fabric8.kubernetes.api.model.ContainerStatus containerStatus);

    String getLog(String podName, long offset) throws IOException;

    io.fabric8.kubernetes.api.model.Container createContainer(String name,
            String image,
            Map<String, String> environments,
            Map<String, String> resourceLimits,
            Map<String, String> resourceRequests,
            List<String> commands,
            List<String> arguments);

    io.fabric8.kubernetes.api.model.PodSpec createPodSpec(io.fabric8.kubernetes.api.model.Container container);

    @Override
    void close();
}
