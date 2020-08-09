package com.trident.load_balancer;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Queues;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jooq.lambda.Unchecked;

import java.net.URI;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.*;

import static com.trident.load_balancer.Component.*;

@Slf4j
public class HeartbeatMonitor extends AbstractStoppable {

    private final Executor heartbeatPollingPool = Executors.newSingleThreadExecutor();

    private final BlockingQueue<HbProcessingTask> heartbeatAckTaskQueue = Queues.newLinkedBlockingQueue();

    private final Cluster cluster;

    public HeartbeatMonitor(final Cluster cluster) {
        this.cluster = cluster;
        handleHeartbeats();
    }

    private void handleHeartbeats() {
        heartbeatPollingPool.execute(Unchecked.runnable(() -> {
            for (; ; ) {
                HbProcessingTask hbProcessingTask = heartbeatAckTaskQueue.take();
                handleHbProcessingTask(hbProcessingTask);
            }
        }));
    }

    private void handleHbProcessingTask(HbProcessingTask hbProcessingTask) {
        try {
            handleHb(hbProcessingTask);
        } catch (Exception e) {
            hbProcessingTask.heartbeatAckFuture.obtrudeException(e);
        }
    }

    private void handleHb(HbProcessingTask hbProcessingTask) {
        ImmutableMap<Component, Number> componentUsage = getValidComponentsForUpdate(hbProcessingTask.heartbeat);
        dispatchHbToNode(componentUsage, hbProcessingTask.uri);
        doHbAck(hbProcessingTask, componentUsage);
    }

    private void doHbAck(HbProcessingTask hbProcessingTask, ImmutableMap<Component, Number> componentUsage) {
        HeartbeatAck ack = new HeartbeatAck(componentUsage.keySet());
        hbProcessingTask.heartbeatAckFuture.complete(ack);
    }

    private void dispatchHbToNode(ImmutableMap<Component, Number> componentUsage, URI nodeUri) {
        Optional<Node> maybeNode = cluster.getNode(nodeUri);
        maybeNode.ifPresent(node -> {
            log.info(String.format("Updating the node %s with new component usage data %s", node, componentUsage));
            for (Entry<Component, Number> componentUsageEntry : componentUsage.entrySet())
                node.updateComponentUsage(componentUsageEntry.getKey(), componentUsageEntry.getValue());
        });
    }

    private ImmutableMap<Component, Number> getValidComponentsForUpdate(Heartbeat hb) {
        ImmutableMap.Builder<Component, Number> components = ImmutableMap.builder();
        Double cpuUsage = hb.getCpuUsage();
        Double ramUsage = hb.getRamUsage();
        Integer connections = hb.getConnections();
        if (isPercentage(cpuUsage))
            components.put(CPU, cpuUsage);
        if (isPercentage(ramUsage))
            components.put(RAM, ramUsage);
        if (isPositive(connections))
            components.put(CONNECTIONS, connections);
        return components.build();
    }

    private boolean isPositive(Integer connections) {
        return connections != null && connections > 0;
    }

    private boolean isPercentage(Double val) {
        return val != null && val >= 0 && val <= 1;
    }

    public Future<HeartbeatAck> onHeartbeat(URI uri, Heartbeat hb) {
        if (isActive()) {
            HbProcessingTask hbProcessingTask = new HbProcessingTask(uri, hb);
            heartbeatAckTaskQueue.add(hbProcessingTask);
            return hbProcessingTask.heartbeatAckFuture;
        } else {
            throw new RuntimeException("Monitor not active!");
        }
    }

    @AllArgsConstructor
    public static class HeartbeatAck {
        private final ImmutableSet<Component> ackedComponents;

        public boolean contains(Component component) {
            return ackedComponents.contains(component);
        }

        public boolean allRecorded() {
            return ackedComponents.size() == Component.values().length;
        }
    }

    private static class HbProcessingTask {
        final URI uri;
        final Heartbeat heartbeat;
        final CompletableFuture<HeartbeatAck> heartbeatAckFuture = new CompletableFuture<>();

        HbProcessingTask(URI uri, Heartbeat heartbeat) {
            this.uri = uri;
            this.heartbeat = heartbeat;
        }
    }
}
