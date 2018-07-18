package io.digdag.standards.command;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.api.client.util.Maps;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.agent.TaskCallbackApi;
import io.digdag.core.archive.ProjectArchive;
import io.digdag.core.archive.ProjectArchiveLoader;
import io.digdag.core.archive.WorkflowResourceMatcher;
import io.digdag.core.storage.StorageManager;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.CommandExecutorContent;
import io.digdag.spi.CommandLogger;
import io.digdag.spi.CommandStatus;
import io.digdag.spi.Storage;
import io.digdag.spi.TaskRequest;
import io.digdag.standards.command.kubernetes.KubernetesClient;
import io.digdag.standards.command.kubernetes.KubernetesClientConfig;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodSpec;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarConstants;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KubernetesCommandExecutor
        implements CommandExecutor
{
    private static final JsonNodeFactory FACTORY = JsonNodeFactory.instance;
    private static Logger logger = LoggerFactory.getLogger(KubernetesCommandExecutor.class);

    private final Config systemConfig;
    private final DockerCommandExecutor docker;
    private final StorageManager storageManager;
    private final TaskCallbackApi callback;
    private final ProjectArchiveLoader projectLoader;
    private final CommandLogger clog;
    private final ConfigFactory cf;

    @Inject
    public KubernetesCommandExecutor(
            final Config systemConfig,
            final DockerCommandExecutor docker,
            final StorageManager storageManager,
            final TaskCallbackApi callback,
            final ProjectArchiveLoader projectLoader,
            final CommandLogger clog,
            final ConfigFactory cf)
    {
        this.systemConfig = systemConfig;
        this.docker = docker;
        this.storageManager = storageManager;
        this.callback = callback;
        this.projectLoader = projectLoader;
        this.clog = clog;
        this.cf = cf;
    }

    // TODO Will be removed before v0.10.x
    @Override
    @Deprecated
    public Process start(final Path projectPath, final TaskRequest request, final ProcessBuilder pb)
            throws IOException
    {
        throw new UnsupportedOperationException("This method is not supported.");
    }

    @Override
    public CommandStatus run(final Path projectPath,
            final Path workspacePath,
            final TaskRequest request,
            final Map<String, String> environments,
            final List<String> commandArguments,
            final String commandId)
            throws IOException, InterruptedException
    {
        final Config config = request.getConfig();
        final KubernetesClientConfig kubernetesClientConfig = createKubernetesClientConfig(config);
        final Storage temporalConfigStorage = createTemporalConfigStorage(config);

        if (kubernetesClientConfig != null && temporalConfigStorage != null) {
            try (final KubernetesClient kubernetesClient = KubernetesClient.create(kubernetesClientConfig)) {
                // Check connectivity with the master of Kubernetes cluster. If true, the client sends a pod request
                // to the master. Otherwise, it falls back and uses DockerCommandExecutor instead.
                if (kubernetesClient.test()) {
                    return runOnKubernetes(projectPath, workspacePath, request, environments, commandArguments,
                            commandId, kubernetesClient, temporalConfigStorage);
                }
            }
        }

        // TODO We'd better to implement chain of responsibility pattern for multiple CommandExecutor selection.

        // Fall back to DockerCommandExecutor
        return docker.run(projectPath, workspacePath, request, environments, commandArguments, commandId);
    }

    @Override
    public CommandStatus poll(final Path projectPath,
            final Path workspacePath,
            final TaskRequest request,
            final CommandStatus previousCommandStatus)
            throws IOException, InterruptedException
    {
        try (final KubernetesClient kubernetesClient = KubernetesClient.create(createKubernetesClientConfig(request.getConfig()))) {
            final KubernetesCommandStatus kubernetesCommandStatus = KubernetesCommandStatus.copyFrom(previousCommandStatus);
            final ObjectNode nextExecutorState = kubernetesCommandStatus.getExecutorState().deepCopy();
            final String commandId = kubernetesCommandStatus.getId();
            final Pod pod = kubernetesClient.getPod(commandId);

            if (logger.isDebugEnabled()) {
                logger.debug("Get pod: " + pod.toString());
            }

            // If the container doesn't start yet, it cannot extract any log messages from the container.
            if (!kubernetesClient.isContainerWaiting(pod.getStatus().getContainerStatuses().get(0))) { // not 'waiting'
                // Read log and write it to CommandLogger
                final ObjectNode executorState = kubernetesCommandStatus.getExecutorState();
                final long offset = !executorState.has("log_offset") ? 0L : executorState.get("log_offset").asLong();
                final String logMessage = kubernetesClient.getLog(commandId, offset);
                log(logMessage, clog);
                nextExecutorState.set("log_offset", FACTORY.numberNode(offset + logMessage.length())); // update log_offset
            }
            else { // 'waiting'
                // Write pod status to the command logger to avoid users confusing. For example, the container waits starting
                // if it will take long time to download container images.
                log(String.format("Wait starting a pod. The current pod phase is {} ...", pod.getStatus().getPhase()), clog);
            }

            // If the Pod completed, it needs to create output contents to pass them to the command executor.
            final String podPhase = pod.getStatus().getPhase();
            final boolean isFinished = podPhase.equals("Succeeded") || podPhase.equals("Failed");
            final Map<String, CommandExecutorContent> outputContents;
            if (isFinished) {
                outputContents = createOutputContents(workspacePath, request, commandId);
            }
            else {
                outputContents = Maps.newHashMap();
            }
            return createCommandStatus(pod, isFinished, nextExecutorState, outputContents);
        }
    }

    private KubernetesClientConfig createKubernetesClientConfig(final Config config)
    {
        try {
            if (config.has("kubernetes")) { // from request config
                return KubernetesClientConfig.createFromTaskRequestConfig(config.getNested("kubernetes")); // ConfigException
            }
            else { // from system config
                return KubernetesClientConfig.createFromSystemConfig(systemConfig); // ConfigException
            }
        }
        catch (ConfigException ignored) {
        }
        return null; // will fall back to DockerCommandExecutor
    }

    private Storage createTemporalConfigStorage(final Config config)
    {
        try {
            if (config.has("kubernetes") && config.getNested("kubernetes").has("config_storage")) { // from request config
                throw new UnsupportedOperationException(); // TODO
            }
            else { // from system config
                // TODO revisit reconsider about parameters name
                return storageManager.create(systemConfig, "agent.command_executor.kubernetes.config_storage."); // ConfigException
            }
        }
        catch (ConfigException ignored) {
        }
        return null; // will fallback to DockerCommandExecutor
    }

    private CommandStatus runOnKubernetes(final Path projectPath,
            final Path workspacePath,
            final TaskRequest request,
            final Map<String, String> environments,
            final List<String> commandArguments,
            final String commandId,
            final KubernetesClient kubernetesClient,
            final Storage configParamStorage)
            throws IOException
    {
        final Config config = request.getConfig();
        final Config dockerConfig = validateDockerConfig(config); // it requires docker: config

        // Build command line arguments that will be passed to Kubernetes API here
        final ImmutableList.Builder<String> beforeArguments = ImmutableList.builder();
        final ImmutableList.Builder<String> afterArguments = ImmutableList.builder();

        beforeArguments.add("set -e");
        beforeArguments.add("mkdir -p " + projectPath.relativize(workspacePath.resolve(".digdag/tmp/" + commandId)).toString());

        // Create project archive on local. Input contents, e.g. input config file and runner script, are included
        // in the project archive. It will be uploaded on temporal config storage and then, will be downloaded on
        // the container. The download URL is generated by pre-signed URL.
        final Path archivePath = createArchiveFromLocal(projectPath, workspacePath, request); // throw IOException
        try {
            // Upload archive on params storage
            final String relativeArchivePath = workspacePath.relativize(archivePath).toString();
            final String archiveKey = createStorageKey(request, commandId, relativeArchivePath);
            uploadFile(archiveKey, archivePath, configParamStorage); // throw IOException
            final String url = getDirectDownloadUrl(archiveKey, configParamStorage);
            beforeArguments.add("curl -s \"" + url + "\" --output " + relativeArchivePath);
            beforeArguments.add("tar -zxf " + relativeArchivePath);
        }
        finally {
            if (archivePath != null) {
                try {
                    Files.deleteIfExists(archivePath); // throw IOException
                }
                catch (IOException e) {
                    logger.warn("Cannot remove a temporal project archive: {}" + archivePath.toString());
                }
            }
        }

        // Upload out file on temporal params storage and generate pre-signed URLs
        {
            final String relativePath = ".digdag/tmp/" + commandId + "/output"; // TODO Base64url encoding
            final Path absoluteOutputPath = workspacePath.resolve(relativePath);
            final String outputFileKey = createStorageKey(request, commandId, relativePath);
            uploadFile(outputFileKey, absoluteOutputPath, configParamStorage); // throw IOException
            final String outputUrl = getDirectUploadUrl(outputFileKey, configParamStorage);
            afterArguments.add(String.format("cat %s | curl -s -X PUT -d @- -H \"Content-Type: application/json\" \"%s\"", relativePath, outputUrl));
        }

        final List<String> commands = Lists.newArrayList("/bin/bash", "-c");
        final List<String> extendedCommandArguments = beforeArguments
                .addAll(commandArguments)
                .addAll(afterArguments.build())
                .build();
        logger.debug("Submit command line arguments to Kubernetes API: " + extendedCommandArguments);

        // Create and submit a pod to Kubernetes master
        final String podName = commandId, containerName = commandId;
        final String containerImage = dockerConfig.get("image", String.class);
        final Pod pod = runPod(kubernetesClient, podName, containerName, containerImage, commands, extendedCommandArguments);
        return createCommandStatus(pod, false, FACTORY.objectNode(), Maps.newHashMap());
    }

    private static void log(final String message, final CommandLogger to)
            throws IOException
    {
        final byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        final InputStream in = new ByteArrayInputStream(bytes);
        to.copy(in, System.out);
    }

    private Map<String, CommandExecutorContent> createOutputContents(final Path workspacePath,
            final TaskRequest request,
            final String commandId)
    {
        final Config config = request.getConfig();
        final Storage configParamsStorage = createTemporalConfigStorage(config);
        final String relativePath = ".digdag/tmp/" + commandId + "/output";
        final long len = workspacePath.resolve(relativePath).toFile().length();
        final String key = createStorageKey(request, commandId, relativePath); // TODO relative key should be encoded by base64url
        final CommandExecutorContent outputContent = KubernetesCommandExecutorContent.create(configParamsStorage, key, len);
        final Map<String, CommandExecutorContent> outputContents = Maps.newHashMap();
        outputContents.put("output", outputContent);
        return outputContents;
    }

    private static Config validateDockerConfig(final Config requestConfig)
    {
        if (!requestConfig.has("docker")) {
            throw new ConfigException("Parameter 'docker' is required but not set");
        }

        final Config dockerConfig = requestConfig.getNested("docker");
        if (!dockerConfig.has("image")) {
            throw new ConfigException("Parameter 'docker.image' is required but not set");
        }

        // TODO revisit to check other config parameters

        return dockerConfig;
    }

    private static String createStorageKey(final TaskRequest request, final String commandId, final String relativePath)
    {
        // file key: {taskId}/{commandId}/{relativePath}
        return new StringBuilder()
                .append(request.getTaskId()).append("/")
                .append(commandId).append("/")
                .append(relativePath)
                .toString();
    }

    private String getDirectDownloadUrl(final String key, final Storage paramsStorage)
    {
        return paramsStorage.getDirectDownloadHandle(key).get().getUrl().toString(); // TODO care of expiry?
    }

    private String getDirectUploadUrl(final String key, final Storage paramsStorage)
    {
        return paramsStorage.getDirectUploadHandle(key).get().getUrl().toString(); // TODO care of expiry?
    }

    private void uploadFile(final String key, final Path filePath, final Storage paramsStorage)
            throws IOException
    {
        final File file = filePath.toFile();
        paramsStorage.put(key, file.length(), () -> new FileInputStream(file));
    }

    /**
     * return relative path of project archive
     *
     * @param projectPath
     * @param workspacePath
     * @param request
     * @return
     * @throws IOException
     */
    private Path createArchiveFromLocal(final Path projectPath, final Path workspacePath, final TaskRequest request)
            throws IOException
    {
        final Path archivePath = Files.createTempFile(workspacePath.resolve(".digdag/tmp"), "archive-", ".tar.gz"); // throw IOException
        logger.debug("Creating " + archivePath + "...");

        final Config config = request.getConfig();
        final WorkflowResourceMatcher workflowResourceMatcher = WorkflowResourceMatcher.defaultMatcher();
        final ProjectArchive projectArchive = projectLoader.load(projectPath, workflowResourceMatcher, config); // throw IOException
        try (final TarArchiveOutputStream tar = new TarArchiveOutputStream(
                new GzipCompressorOutputStream(Files.newOutputStream(archivePath.toAbsolutePath())))) { // throw IOException
            tar.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);
            projectArchive.listAllFiles((resourceName, absPath) -> { // throw IOException
                if (!Files.isDirectory(absPath)) {
                    logger.debug("  Archiving " + resourceName);

                    final TarArchiveEntry e = buildTarArchiveEntry(projectPath, absPath, resourceName); // throw IOException
                    tar.putArchiveEntry(e); // throw IOException
                    if (e.isSymbolicLink()) {
                        logger.debug("    symlink -> " + e.getLinkName());
                    }
                    else {
                        try (final InputStream in = Files.newInputStream(absPath)) { // throw IOException
                            ByteStreams.copy(in, tar); // throw IOException
                        }
                    }
                    tar.closeArchiveEntry(); // throw IOExcpetion
                }
            });
        }

        return archivePath;
    }

    private TarArchiveEntry buildTarArchiveEntry(final Path projectPath, final Path absPath, final String name)
            throws IOException
    {
        final TarArchiveEntry e;
        if (Files.isSymbolicLink(absPath)) {
            e = new TarArchiveEntry(name, TarConstants.LF_SYMLINK);
            final Path rawDest = Files.readSymbolicLink(absPath);
            final Path normalizedAbsDest = absPath.getParent().resolve(rawDest).normalize();

            if (!normalizedAbsDest.startsWith(projectPath)) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                        "Invalid symbolic link: Given path '%s' is outside of project directory '%s'", normalizedAbsDest, projectPath));
            }

            // absolute path will be invalid on a server. convert it to a relative path
            final Path normalizedRelativeDest = absPath.getParent().relativize(normalizedAbsDest);

            String linkName = normalizedRelativeDest.toString();

            // TarArchiveEntry(File) does this normalization but setLinkName doesn't. So do it here:
            linkName = linkName.replace(File.separatorChar, '/');
            e.setLinkName(linkName);
        }
        else {
            e = new TarArchiveEntry(absPath.toFile(), name);
            try {
                int mode = 0;
                for (final PosixFilePermission perm : Files.getPosixFilePermissions(absPath)) {
                    switch (perm) {
                        case OWNER_READ:
                            mode |= 0400;
                            break;
                        case OWNER_WRITE:
                            mode |= 0200;
                            break;
                        case OWNER_EXECUTE:
                            mode |= 0100;
                            break;
                        case GROUP_READ:
                            mode |= 0040;
                            break;
                        case GROUP_WRITE:
                            mode |= 0020;
                            break;
                        case GROUP_EXECUTE:
                            mode |= 0010;
                            break;
                        case OTHERS_READ:
                            mode |= 0004;
                            break;
                        case OTHERS_WRITE:
                            mode |= 0002;
                            break;
                        case OTHERS_EXECUTE:
                            mode |= 0001;
                            break;
                        default:
                            // ignore
                    }
                }
                e.setMode(mode);
            }
            catch (UnsupportedOperationException ex) {
                // ignore custom mode
            }
        }
        return e;
    }

    private Pod runPod(final KubernetesClient client,
            final String podName,
            final String containerName,
            final String containerImage,
            final List<String> containerCommands,
            final List<String> containerArguments)
    {
        final Container container = client.createContainer(containerName, containerImage, containerCommands, containerArguments);
        final PodSpec podSpec = client.createPodSpec(container);
        return client.runPod(podName, ImmutableMap.of(), podSpec);
    }

    private KubernetesCommandStatus createCommandStatus(final Pod pod,
            final boolean isFinished,
            final ObjectNode executorState,
            final Map<String, CommandExecutorContent> outputContents)
    {
        return KubernetesCommandStatus.create(pod, isFinished, executorState, outputContents);
    }
}
