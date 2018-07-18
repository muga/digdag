package io.digdag.standards.command.kubernetes;

import io.digdag.client.config.Config;

public class KubernetesClientConfig
{
    public static KubernetesClientConfig createFromSystemConfig(final io.digdag.client.config.Config systemConfig)
    {
        // TODO throw config exception if parameters are missed
        final String configKeyPrefix = "agent.command_executor.kubernetes.";
        if (!systemConfig.has(configKeyPrefix + "master") ||
                !systemConfig.has(configKeyPrefix + "certs_ca_data") ||
                !systemConfig.has(configKeyPrefix + "oauth_token") ||
                !systemConfig.has(configKeyPrefix + "namespace")) {
            return null;
        }

        return create(systemConfig.get(configKeyPrefix + "master", String.class),
                systemConfig.get(configKeyPrefix + "certs_ca_data", String.class),
                systemConfig.get(configKeyPrefix + "oauth_token", String.class),
                systemConfig.get(configKeyPrefix + "namespace", String.class));
    }

    public static KubernetesClientConfig createFromTaskRequestConfig(final Config config)
    {
        // TODO throw config exception if parameters are missed
        if (!config.has("master") ||
                !config.has("certs_ca_data") ||
                !config.has("oauth_token") ||
                !config.has("namespace")) {
            return null;
        }

        return create(config.get("master", String.class),
                config.get("certs_ca_data", String.class),
                config.get("oauth_token", String.class),
                config.get("namespace", String.class));
    }

    private static KubernetesClientConfig create(final String master,
            final String certsCaData,
            final String oauthToken,
            final String namespace)
    {
        System.setProperty("kubernetes.auth.tryKubeConfig", "false");
        return new KubernetesClientConfig(master, namespace, certsCaData, oauthToken);
    }

    private final String master;
    private final String namespace;
    private final String certsCaData;
    private final String oauthToken;

    private KubernetesClientConfig(final String master,
            final String namespace,
            final String certsCaData,
            final String oauthToken)
    {
        this.master = master;
        this.namespace = namespace;
        this.certsCaData = certsCaData;
        this.oauthToken = oauthToken;
    }

    public String getMaster()
    {
        return this.master;
    }

    public String getNamespace()
    {
        return this.namespace;
    }

    public String getCertsCaData()
    {
        return this.certsCaData;
    }

    public String getOauthToken()
    {
        return this.oauthToken;
    }
}
