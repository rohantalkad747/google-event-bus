package com.trident.load_balancer;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jooq.lambda.Unchecked;

import java.net.InetAddress;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.*;

import static com.trident.load_balancer.Component.*;

@Slf4j
public class HeartbeatMonitor extends AbstractStoppable {

    private final Map<InetAddress, Long> lastHeartbeat = Maps.newHashMap();

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
                if (!isActive())
                    return;
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
        Long hbEpochMs = hbProcessingTask.heartbeat.getTimeEpochMs();
        Long lastHeartbeatEpochMs = lastHeartbeat.computeIfAbsent(hbProcessingTask.ipAddress, k -> hbEpochMs);
        if (hbEpochMs < lastHeartbeatEpochMs) {
            throw new RuntimeException(
                    String.format(
                            "Heartbeat (timestamp=%s) is out of sync with the most latest processed heartbeat" +
                                    " processed heartbeat (timestamp=%s) for node with ip %s!",
                            hbEpochMs,
                            lastHeartbeatEpochMs,
                            hbProcessingTask.ipAddress
                    )
            );
        }
        ImmutableMap<Component, Number> componentUsage = getValidComponentsForUpdate(hbProcessingTask.heartbeat);
        dispatchHbToNode(componentUsage, hbProcessingTask.ipAddress);
        doHbAck(hbProcessingTask, componentUsage);
    }

    private void doHbAck(HbProcessingTask hbProcessingTask, ImmutableMap<Component, Number> componentUsage) {
        HeartbeatAck ack = new HeartbeatAck(componentUsage.keySet());
        hbProcessingTask.heartbeatAckFuture.complete(ack);
    }

    private void dispatchHbToNode(ImmutableMap<Component, Number> componentUsage, InetAddress nodeUri) {
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

    public Future<HeartbeatAck> onHeartbeat(InetAddress inetAddress, Heartbeat hb) {
        if (isActive()) {
            return processHb(inetAddress, hb);
        } else {
            throw new RuntimeException("Monitor not active!");
        }
    }

    private Future<HeartbeatAck> processHb(InetAddress inetAddress, Heartbeat hb) {
        HbProcessingTask hbProcessingTask = new HbProcessingTask(inetAddress, hb);
        heartbeatAckTaskQueue.add(hbProcessingTask);
        return hbProcessingTask.heartbeatAckFuture;
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
        final InetAddress ipAddress;
        final Heartbeat heartbeat;
        final CompletableFuture<HeartbeatAck> heartbeatAckFuture = new CompletableFuture<>();

        HbProcessingTask(InetAddress ipAddress, Heartbeat heartbeat) {
            this.ipAddress = ipAddress;
            this.heartbeat = heartbeat;
        }
    }
}
