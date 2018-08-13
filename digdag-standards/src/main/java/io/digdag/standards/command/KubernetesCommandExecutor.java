package io.digdag.standards.command;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.core.archive.ProjectArchive;
import io.digdag.core.archive.ProjectArchiveLoader;
import io.digdag.core.archive.ProjectArchives;
import io.digdag.core.archive.WorkflowResourceMatcher;
import io.digdag.core.storage.StorageManager;
import io.digdag.spi.CommandContext;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.CommandLogger;
import io.digdag.spi.CommandRequest;
import io.digdag.spi.CommandStatus;
import io.digdag.spi.Storage;
import io.digdag.spi.StorageFileNotFoundException;
import io.digdag.spi.StorageObject;
import io.digdag.spi.TaskRequest;
import io.digdag.standards.command.kubernetes.KubernetesClient;
import io.digdag.standards.command.kubernetes.KubernetesClientConfig;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerStateTerminated;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.PodStatus;
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
    private final ProjectArchiveLoader projectLoader;
    private final CommandLogger clog;

    @Inject
    public KubernetesCommandExecutor(
            final Config systemConfig,
            final DockerCommandExecutor docker,
            final StorageManager storageManager,
            final ProjectArchiveLoader projectLoader,
            final CommandLogger clog)
    {
        this.systemConfig = systemConfig;
        this.docker = docker;
        this.storageManager = storageManager;
        this.projectLoader = projectLoader;
        this.clog = clog;
    }

    @Override
    public CommandStatus run(final CommandContext context, final CommandRequest request)
            throws IOException
    {
        final Config config = context.getTaskRequest().getConfig();
        final KubernetesClientConfig kubernetesClientConfig = createKubernetesClientConfig(config);
        final Storage temporalConfigStorage = createTemporalConfigStorage(config);

        if (kubernetesClientConfig != null && temporalConfigStorage != null) {
            try (final KubernetesClient kubernetesClient = KubernetesClient.create(kubernetesClientConfig)) {
                // Check connectivity with the master of Kubernetes cluster. If true, the client sends a pod request
                // to the master. Otherwise, it falls back and uses DockerCommandExecutor instead.
                if (kubernetesClient.test()) {
                    return runOnKubernetes(context, request, kubernetesClient, temporalConfigStorage);
                }
            }
        }

        // TODO We'd better to implement chain of responsibility pattern for multiple CommandExecutor selection.

        // Fall back to DockerCommandExecutor
        return docker.run(context, request);
    }

    @Override
    public CommandStatus poll(final CommandContext context,
            final ObjectNode previousStatusJson)
            throws IOException
    {
        final Config config = context.getTaskRequest().getConfig();
        try (final KubernetesClient kubernetesClient = KubernetesClient.create(createKubernetesClientConfig(config))) {
            final String podName = previousStatusJson.get("pod_name").asText();
            final Pod pod = kubernetesClient.getPod(podName);

            if (logger.isDebugEnabled()) {
                logger.debug("Get pod: " + pod.toString());
            }

            final ObjectNode previousExecutorState = (ObjectNode) previousStatusJson.get("executor_state");
            final ObjectNode nextExecutorState = previousExecutorState.deepCopy();
            // If the container doesn't start yet, it cannot extract any log messages from the container.
            if (!kubernetesClient.isContainerWaiting(pod.getStatus().getContainerStatuses().get(0))) { // not 'waiting'
                // Read log and write it to CommandLogger
                final long offset = !previousExecutorState.has("log_offset") ? 0L : previousExecutorState.get("log_offset").asLong();
                final String logMessage = kubernetesClient.getLog(podName, offset);
                log(logMessage, clog);
                nextExecutorState.set("log_offset", FACTORY.numberNode(offset + logMessage.length())); // update log_offset
            }
            else { // 'waiting'
                // Write pod status to the command logger to avoid users confusing. For example, the container
                // waits starting if it will take long time to download container images.
                log(String.format("Wait starting a pod. The current pod phase is {} ...", pod.getStatus().getPhase()), clog);
            }

            final ObjectNode nextStatusJson = previousStatusJson.deepCopy();
            nextStatusJson.set("executor_state", nextExecutorState);

            // If the Pod completed, it needs to create output contents to pass them to the command executor.
            final String podPhase = pod.getStatus().getPhase();
            final boolean isFinished = podPhase.equals("Succeeded") || podPhase.equals("Failed");

            if (isFinished) {
                final String ioDirectoryPathName = nextStatusJson.get("io_directory").asText();
                final String outputArchivePathName = ".digdag/tmp/archive-output.tar.gz";
                final String outputArchiveKey = createStorageKey(context.getTaskRequest(), ioDirectoryPathName, outputArchivePathName); // url format

                final Storage temporalConfigStorage = createTemporalConfigStorage(config);
                // Download output config archive
                try {
                    final StorageObject so = temporalConfigStorage.open(outputArchiveKey);
                    final InputStream in = so.getContentInputStream();
                    ProjectArchives.extractTarArchive(context.getLocalProjectPath(), in);
                }
                catch (StorageFileNotFoundException e) {
                    throw Throwables.propagate(e);
                }
            }

            return createCommandStatus(pod, isFinished, nextStatusJson);
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

    private CommandStatus runOnKubernetes(final CommandContext context,
            final CommandRequest request,
            final KubernetesClient kubernetesClient,
            final Storage configParamStorage)
            throws IOException
    {
        final Path projectPath = context.getLocalProjectPath();
        final Path ioDirectoryPath = request.getIoDirectory(); // relative
        final Config config = context.getTaskRequest().getConfig();
        final Config dockerConfig = validateDockerConfig(config); // it requires docker: config

        // Build command line arguments that will be passed to Kubernetes API here
        final ImmutableList.Builder<String> bashArguments = ImmutableList.builder();

        //bashArguments.add("set -eux"); // TODO revisit we need it or not
        bashArguments.add("mkdir -p " + ioDirectoryPath.toString());

        // Create project archive on local. Input contents, e.g. input config file and runner script, are included
        // in the project archive. It will be uploaded on temporal config storage and then, will be downloaded on
        // the container. The download URL is generated by pre-signed URL.
        final Path archivePath = createArchiveFromLocal(projectPath, request.getIoDirectory(), context.getTaskRequest());
        final String archivePathName = projectPath.relativize(archivePath).toString(); // relative
        try {
            // Upload archive on params storage
            final String archiveKey = createStorageKey(context.getTaskRequest(), ioDirectoryPath.toString(), archivePathName); // TODO url format
            uploadFile(archiveKey, archivePath, configParamStorage); // throw IOException
            final String url = getDirectDownloadUrl(archiveKey, configParamStorage);
            bashArguments.add("curl -s \"" + url + "\" --output " + archivePathName);
            bashArguments.add("tar -zxf " + archivePathName);
            final String pushdDir = request.getWorkingDirectory().toString();
            bashArguments.add("pushd " + (pushdDir.isEmpty() ? "." : pushdDir));
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

        // Add command passed from operator.
        bashArguments.add(request.getCommandLine().stream().map(Object::toString).collect(Collectors.joining(" ")));
        bashArguments.add("exit_code=$?");

        // Create project archive path in the container
        // Upload the archive file to the S3 bucket
        final String outputArchivePathName = ".digdag/tmp/archive-output.tar.gz";
        final String outputArchiveKey = createStorageKey(context.getTaskRequest(), ioDirectoryPath.toString(), outputArchivePathName); // url format
        final String url = getDirectUploadUrl(outputArchiveKey, configParamStorage);
        bashArguments.add("popd");
        bashArguments.add(String.format("tar -zcf %s  --exclude %s --exclude %s .digdag/tmp/", outputArchivePathName, archivePathName, outputArchivePathName));
        bashArguments.add(String.format("curl -s -X PUT -T %s -L \"%s\"", outputArchivePathName, url));
        bashArguments.add("exit $exit_code");

        final List<String> commands = ImmutableList.of("/bin/bash");
        final List<String> arguments = ImmutableList.of("-c",
                bashArguments.build().stream().map(Object::toString).collect(Collectors.joining("; ")));
        logger.debug("Submit command line arguments to Kubernetes API: " + arguments);

        // Create and submit a pod to Kubernetes master
        final String podName = createUniquePodName(context.getTaskRequest());
        final String containerImage = dockerConfig.get("image", String.class);
        final Pod pod = runPod(kubernetesClient, podName, containerImage, request.getEnvironments(), commands, arguments);

        final ObjectNode nextStatus = FACTORY.objectNode();
        nextStatus.set("pod_name", FACTORY.textNode(pod.getMetadata().getName()));
        nextStatus.set("io_directory", FACTORY.textNode(ioDirectoryPath.toString()));
        nextStatus.set("executor_state", FACTORY.objectNode());
        return createCommandStatus(pod, false, nextStatus);
    }

    private static void log(final String message, final CommandLogger to)
            throws IOException
    {
        final byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        final InputStream in = new ByteArrayInputStream(bytes);
        to.copy(in, System.out);
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

    private static String createStorageKey(final TaskRequest request,
            final String ioDirectoryName,
            final String relativePath)
    {
        // file key: {taskId}/{commandId}/{relativePath}
        return new StringBuilder()
                .append(request.getTaskId()).append("/")
                .append(ioDirectoryName).append("/")
                .append(relativePath)
                .toString();
    }

    private static String createUniquePodName(final TaskRequest request)
    {
        // name format: digdag-py-{taskId}-{UUIDv4}
        final long taskId = request.getTaskId();
        return new StringBuilder()
                .append("digdag-py-")
                .append(taskId).append("-")
                .append(UUID.randomUUID().toString()) // UUID v4
                .toString();
    }

    private String getDirectDownloadUrl(final String key, final Storage paramsStorage)
    {
        // TODO care of expiry? server-side encryption
        return paramsStorage.getDirectDownloadHandle(key).get().getUrl().toString();
    }

    private String getDirectUploadUrl(final String key, final Storage paramsStorage)
    {
        // TODO care of expiry? server-side encryption
        return paramsStorage.getDirectUploadHandle(key).get().getUrl().toString();
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
     * @param request
     * @return
     * @throws IOException
     */
    private Path createArchiveFromLocal(final Path projectPath, final Path ioDirectoryPath, final TaskRequest request)
            throws IOException
    {
        final Path archivePath = Files.createTempFile(projectPath.resolve(".digdag/tmp"), "archive-input-", ".tar.gz"); // throw IOException
        logger.debug("Creating " + archivePath + "...");

        final Config config = request.getConfig();
        final WorkflowResourceMatcher workflowResourceMatcher = WorkflowResourceMatcher.defaultMatcher();
        final ProjectArchive projectArchive = projectLoader.load(projectPath, workflowResourceMatcher, config); // throw IOException
        try (final TarArchiveOutputStream tar = new TarArchiveOutputStream(
                new GzipCompressorOutputStream(Files.newOutputStream(archivePath.toAbsolutePath())))) { // throw IOException
            // Archive project files
            tar.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);
            projectArchive.listFiles((resourceName, absPath) -> { // throw IOException
                logger.debug("  Archiving " + resourceName);
                if (!Files.isDirectory(absPath)) {
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

            // Add .digdag/tmp/ files to the archive
            final Path absoluteIoDirectoryPath = projectPath.resolve(ioDirectoryPath);
            try (final DirectoryStream<Path> ds = Files.newDirectoryStream(absoluteIoDirectoryPath)) {
                for (final Path absPath : ds) {
                    final String resourceName = ProjectArchive.realPathToResourceName(projectPath, absPath);
                    final TarArchiveEntry e = buildTarArchiveEntry(projectPath, absPath, resourceName);
                    tar.putArchiveEntry(e); // throw IOexception
                    if (!e.isSymbolicLink()) {
                        try (final InputStream in = Files.newInputStream(absPath)) { // throw IOException
                            ByteStreams.copy(in, tar); // throw IOException
                        }
                    }
                    tar.closeArchiveEntry(); // throw IOExcpetion
                }
            }
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
            final String containerImage,
            final Map<String, String> environments,
            final List<String> containerCommands,
            final List<String> containerArguments)
    {
        // container name and pod name are same
        final Container container = client.createContainer(podName, containerImage, environments, containerCommands, containerArguments);
        final PodSpec podSpec = client.createPodSpec(container);
        return client.runPod(podName, ImmutableMap.of(), podSpec);
    }

    private CommandStatus createCommandStatus(final Pod pod,
            final boolean isFinished,
            final ObjectNode nextStatus)
    {
        // we don't need to update "io_directory"

        if (isFinished) {
            // Extract status code from container status
            final PodStatus podStatus = pod.getStatus();
            final List<ContainerStatus> containerStatusList = podStatus.getContainerStatuses();
            final ContainerStateTerminated terminated = containerStatusList.get(0).getState().getTerminated();
            nextStatus.set("status_code", FACTORY.numberNode(terminated.getExitCode()));
        }

        return KubernetesCommandStatus.of(isFinished, nextStatus);
    }
}
