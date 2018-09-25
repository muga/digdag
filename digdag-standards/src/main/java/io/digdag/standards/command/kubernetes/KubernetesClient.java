package io.digdag.standards.command.kubernetes;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.dsl.PodResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
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
        return new KubernetesClient(kubernetesClientConfig, new DefaultKubernetesClient(clientConfig));
    }

    private final KubernetesClientConfig config;
    private final DefaultKubernetesClient client;

    private KubernetesClient(final KubernetesClientConfig config, final DefaultKubernetesClient client) {
        this.config = config;
        this.client = client;
    }

    public KubernetesClientConfig getConfig()
    {
        return config;
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
                //.withHostNetwork(true);
                //.withDnsPolicy("ClusterFirstWithHostNet");
                .addToContainers(container)
                // TODO extract as config parameter
                // Restart policy is "Never" by default since it needs to avoid executing the operator multiple times. It might not
                // make the script idempotent.
                // https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#restart-policy
                .withRestartPolicy("Never")
                .build();
    }

    public Container createContainer(
            final String name,
            final String image,
            final Map<String, String> environments,
            final Map<String, String> resourceLimits,
            final Map<String, String> resourceRequests,
            final List<String> commands,
            final List<String> arguments)
    {
        return new ContainerBuilder()
                .withEnv(toEnvVars(environments))
                .withName(name)
                .withImage(image)
                .withResources(toResourceRequirements(resourceLimits, resourceRequests))
                .withCommand(commands)
                .withArgs(arguments)
                .build();
    }

    private static List<EnvVar> toEnvVars(final Map<String, String> environments)
    {
        final ImmutableList.Builder<EnvVar> envVars = ImmutableList.builder();
        for (final Map.Entry<String, String> e : environments.entrySet()) {
            final EnvVar envVar = new EnvVarBuilder().withName(e.getKey()).withValue(e.getValue()).build();
            envVars.add(envVar);
        }
        return envVars.build();
    }

    private static ResourceRequirements toResourceRequirements(
            final Map<String, String> limits,
            final Map<String, String> requests)
    {
        final ImmutableMap.Builder<String, Quantity> ls = new ImmutableMap.Builder<>();
        for (Map.Entry<String, String> e : limits.entrySet()) {
            ls.put(e.getKey(), new Quantity(e.getValue()));
        }

        final ImmutableMap.Builder<String, Quantity> rs = new ImmutableMap.Builder<>();
        for (Map.Entry<String, String> e : requests.entrySet()) {
            rs.put(e.getKey(), new Quantity(e.getValue()));
        }

        return new ResourceRequirements(ls.build(), rs.build());
    }

    @Override
    public void close()
    {
        if (client != null) {
            client.close();
        }
    }
}
