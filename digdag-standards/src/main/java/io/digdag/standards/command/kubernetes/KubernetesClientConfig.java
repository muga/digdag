package io.digdag.standards.command.kubernetes;

import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.core.storage.StorageManager;

public class KubernetesClientConfig
{
    private static final String KUBERNETES_CLIENT_PARAMS_PREFIX = "agent.command_executor.kubernetes.";

    public static KubernetesClientConfig create(final Config systemConfig, final Config requestConfig)
            throws ConfigException
    {
        if (requestConfig.has("kubernetes")) {
            // from task request config
            return KubernetesClientConfig.createFromTaskRequestConfig(requestConfig.getNested("kubernetes"));
        }
        else {
            // from system config
            return KubernetesClientConfig.createFromSystemConfig(systemConfig);
        }
    }

    private static KubernetesClientConfig createFromTaskRequestConfig(final Config config)
    {
        validateParams(config);
        return create(config.get("name", String.class, "default"),
                config.get("master", String.class),
                config.get("certs_ca_data", String.class),
                config.get("oauth_token", String.class),
                config.get("namespace", String.class));
    }

    private static KubernetesClientConfig createFromSystemConfig(final io.digdag.client.config.Config systemConfig)
    {
        final String name = systemConfig.get(KUBERNETES_CLIENT_PARAMS_PREFIX + "name", String.class, "default");
        final String keyPrefix = "default".equals(name) ? KUBERNETES_CLIENT_PARAMS_PREFIX : KUBERNETES_CLIENT_PARAMS_PREFIX + name + ".";
        final Config extracted = validateParams(StorageManager.extractKeyPrefix(systemConfig, keyPrefix));
        return create(name,
                extracted.get("master", String.class),
                extracted.get("certs_ca_data", String.class),
                extracted.get("oauth_token", String.class),
                extracted.get("namespace", String.class));
    }

    private static Config validateParams(final Config config)
    {
        if (!config.has("master") ||
                !config.has("certs_ca_data") ||
                !config.has("oauth_token") ||
                !config.has("namespace")) {
            throw new ConfigException("kubernetes config must have master:, certs_ca_data:, oauth_token: and namespace:");
        }
        return config;
    }

    private static KubernetesClientConfig create(final String name,
            final String master,
            final String certsCaData,
            final String oauthToken,
            final String namespace)
    {
        System.setProperty("kubernetes.auth.tryKubeConfig", "false");
        return new KubernetesClientConfig(name, master, namespace, certsCaData, oauthToken);
    }

    private final String name;
    private final String master;
    private final String namespace;
    private final String certsCaData;
    private final String oauthToken;

    private KubernetesClientConfig(final String name,
            final String master,
            final String namespace,
            final String certsCaData,
            final String oauthToken)
    {
        this.name = name;
        this.master = master;
        this.namespace = namespace;
        this.certsCaData = certsCaData;
        this.oauthToken = oauthToken;
    }

    public String getName()
    {
        return this.name;
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
