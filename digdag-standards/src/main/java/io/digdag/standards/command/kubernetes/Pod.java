package io.digdag.standards.command.kubernetes;

import io.fabric8.kubernetes.api.model.ContainerStateTerminated;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.PodStatus;

import java.util.List;

public class Pod
{
    public static Pod of(final io.fabric8.kubernetes.api.model.Pod pod)
    {
        return new Pod(pod);
    }

    private final io.fabric8.kubernetes.api.model.Pod pod;

    private Pod(final io.fabric8.kubernetes.api.model.Pod pod)
    {
        this.pod = pod;
    }

    public String getName()
    {
        return getMetadata().getName();
    }

    public String getPhase()
    {
        return getStatus().getPhase();
    }

    public int getStatusCode()
    {
        // if the pod completed, we can use this method.
        final PodStatus podStatus = pod.getStatus();
        final List<ContainerStatus> containerStatusList = podStatus.getContainerStatuses();
        final ContainerStateTerminated terminated = containerStatusList.get(0).getState().getTerminated();
        return terminated.getExitCode();
    }

    PodStatus getStatus()
    {
        return pod.getStatus();
    }

    ObjectMeta getMetadata()
    {
        return pod.getMetadata();
    }
}
