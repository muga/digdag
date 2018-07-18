package io.digdag.standards.command.kubernetes;

import com.google.common.collect.Lists;
import com.google.common.io.CharStreams;
import io.digdag.spi.CommandLogger;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.PodStatus;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.dsl.PodResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class KubernetesClient
        implements AutoCloseable
{
    private static Logger logger = LoggerFactory.getLogger(KubernetesClient.class);

    public static KubernetesClient create(final KubernetesClientConfig kubernetesClientConfig)
    {
        final Config clientConfig = new ConfigBuilder()
                .withMasterUrl(kubernetesClientConfig.getMaster())
                .withNamespace(kubernetesClientConfig.getNamespace())
                .withCaCertData(kubernetesClientConfig.getCertsCaData())
                .withOauthToken(kubernetesClientConfig.getOauthToken())
                .build();
        return new KubernetesClient(new DefaultKubernetesClient(clientConfig));
    }

    private final DefaultKubernetesClient client;

    private KubernetesClient(final DefaultKubernetesClient client) {
        this.client = client;
    }

    public boolean test()
    {
        // TODO better to check connectivity with master of specified Kubernetes cluster
        return true;
    }

    public Pod runPod(final String podName,
            final Map<String, String> podLabels,
            final PodSpec podSpec)
    {
        return client.pods()
                .inNamespace(client.getNamespace())
                .createNew()
                .withNewMetadata()
                .withName(podName)
                .withLabels(podLabels)
                .endMetadata()
                .withSpec(podSpec)
                .done();
    }

    public Pod getPod(final String podName)
    {
        return client.pods()
                .inNamespace(client.getNamespace())
                .withName(podName)
                .get();
    }

    public boolean isContainerWaiting(final ContainerStatus status)
    {
        return status.getState().getWaiting() != null;
    }

    public String getLog(final String podName, final long offset)
            throws IOException
    {
        final PodResource podResource = client.pods().withName(podName);
        final Reader reader = podResource.getLogReader(); // return InputStreamReader
        try {
            reader.skip(offset); // skip the chars that were already read
            return CharStreams.toString(reader); // TODO not use String object
        }
        finally {
            reader.close();
        }
    }

    public PodSpec createPodSpec(final Container container)
    {
        return new PodSpecBuilder()
                .addToContainers(container)
                //.withHostNetwork(true);
                //.withDnsPolicy("ClusterFirstWithHostNet");
                .withRestartPolicy("Never") // TODO explain this
                .build();
    }

    public Container createContainer(
            final String name,
            final String image,
            final List<String> commands,
            final List<String> arguments)
    {
        return new ContainerBuilder()
                .withName(name)
                .withImage(image)
                .withCommand(commands)
                .withArgs(arguments.stream().map(Object::toString).collect(Collectors.joining("; ")))
                .build();
    }

    @Override
    public void close()
    {
        if (client != null) {
            client.close();
        }
    }
}
