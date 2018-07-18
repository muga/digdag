package io.digdag.standards.command;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.api.client.util.Maps;
import io.digdag.spi.CommandExecutorContent;
import io.digdag.spi.CommandStatus;
import io.fabric8.kubernetes.api.model.ContainerStateTerminated;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodStatus;
import java.util.List;
import java.util.Map;

public class KubernetesCommandStatus
        extends CommandStatus
{
    private static final JsonNodeFactory FACTORY = JsonNodeFactory.instance;

    static KubernetesCommandStatus copyFrom(final CommandStatus commandStatus)
    {
        return new KubernetesCommandStatus(commandStatus.getObjectNode().deepCopy(), Maps.newHashMap());
    }

    static KubernetesCommandStatus create(final Pod pod,
            final boolean isFinished,
            final ObjectNode executorState,
            final Map<String, CommandExecutorContent> outputContents)
    {
        final ObjectNode object = FACTORY.objectNode();
        object.set("id", FACTORY.textNode(pod.getMetadata().getName()));
        object.set("is_finished", FACTORY.booleanNode(isFinished));
        object.set("executor_state", executorState);

        // Extract status code from container status
        final PodStatus podStatus = pod.getStatus();
        if (isFinished && !podStatus.getContainerStatuses().isEmpty()) {
            final List<ContainerStatus> containerStatusList = podStatus.getContainerStatuses();
            final ContainerStateTerminated terminated = containerStatusList.get(0).getState().getTerminated();
            int statusCode = terminated.getExitCode();
            object.set("status_code", FACTORY.numberNode(statusCode));
        }

        return new KubernetesCommandStatus(object, outputContents);
    }

    private final Map<String, CommandExecutorContent> outputContents;

    private KubernetesCommandStatus(final ObjectNode object,
            final Map<String, CommandExecutorContent> outputContents)
    {
        super(object);
        this.outputContents = outputContents;
    }

    public String getId()
    {
        return object.get("id").asText();
    }

    public ObjectNode getExecutorState()
    {
        return (ObjectNode) object.get("executor_state");
    }

    @Override
    public CommandExecutorContent getOutputContent(final String path)
    {
        return outputContents.get(path);
    }
}