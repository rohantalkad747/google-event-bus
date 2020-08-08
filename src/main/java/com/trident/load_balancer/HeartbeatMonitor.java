package com.trident.load_balancer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Queues;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.jooq.lambda.Unchecked;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.*;

import static com.trident.load_balancer.Component.*;

public class HeartbeatMonitor extends AbstractStoppable {
    private final Executor heartbeatPollingPool = Executors.newSingleThreadExecutor();
    private final BlockingQueue<HbAckTask> heartbeatAckTaskQueue = Queues.newLinkedBlockingQueue();

    private final Cluster cluster;

    public HeartbeatMonitor(final Cluster cluster) {
        this.cluster = cluster;
        handleHeartbeats();
    }

    @AllArgsConstructor
    @Data
    static class HeartbeatAck {
        private final ImmutableList<Component> ackedComponents;

        static class Builder {
            ImmutableList.Builder<Component> ackedComponents = ImmutableList.builder();

            Builder withAck(Component component) {
                ackedComponents.add(component);
                return this;
            }

            HeartbeatAck build() {
                return new HeartbeatAck(ackedComponents.build());
            }
        }

        public boolean allRecorded() {
            return ackedComponents.size() == Component.values().length;
        }
    }

    private void handleHeartbeats() {
        heartbeatPollingPool.execute(Unchecked.runnable(() -> {
            for (; ; ) {
                HbAckTask hbAckTask = heartbeatAckTaskQueue.take();
                handleNewAckTask(hbAckTask);
            }
        }));
    }

    private void handleNewAckTask(HbAckTask hbAckTask) {
        Heartbeat hb = hbAckTask.heartbeat;
        try {
            HeartbeatAck ack = buildAck(hb);
            hbAckTask.heartbeatAckFuture.complete(ack);
        } catch (Exception e) {
            hbAckTask.heartbeatAckFuture.failedFuture(e);
        } finally {
            URI nodeUri = hbAckTask.uri;
            Optional<Node> maybeNode = cluster.getNode(nodeUri);
            maybeNode.ifPresent((node) -> {
                node.
            });
        }
    }

    private HeartbeatAck buildAck(Heartbeat hb) {
        double cpuUsage = hb.getCpuUsage();
        double ramUsage = hb.getCpuUsage();
        int connections = hb.getConnections();
        HeartbeatAck.Builder heartbeatAckBuilder = new HeartbeatAck.Builder();
        addAcks(cpuUsage, ramUsage, connections, heartbeatAckBuilder);
        return heartbeatAckBuilder.build();
    }

    private void addAcks(double cpuUsage, double ramUsage, int connections, HeartbeatAck.Builder heartbeatAckBuilder) {
        addAckIfPercentage(heartbeatAckBuilder, cpuUsage, CPU);
        addAckIfPercentage(heartbeatAckBuilder, ramUsage, RAM);
        addAckIfPositive(connections, heartbeatAckBuilder);
    }

    private void addAckIfPositive(int connections, HeartbeatAck.Builder heartbeatAckBuilder) {
        if (connections >= 0) {
            heartbeatAckBuilder.ackedComponents.add(CONNECTIONS);
        }
    }

    private void addAckIfPercentage(HeartbeatAck.Builder heartbeatAckBuilder, double usage, Component component) {
        if (isPercentage(usage)) {
            heartbeatAckBuilder.ackedComponents.add(component);
        }
    }


    private boolean isPercentage(double val) {
        return val >= 0 && val <= 1;
    }

    private static class HbAckTask {
        final URI uri;
        final Heartbeat heartbeat;
        final CompletableFuture<HeartbeatAck> heartbeatAckFuture = new CompletableFuture<>();

        HbAckTask(URI uri, Heartbeat heartbeat) {
            this.uri = uri;
            this.heartbeat = heartbeat;
        }
    }

    public Future<HeartbeatAck> onHeartbeat(URI uri, Heartbeat hb) {
        HbAckTask hbAckTask = new HbAckTask(uri, hb);
        heartbeatAckTaskQueue.add(hbAckTask);
        return hbAckTask.heartbeatAckFuture;
    }
}
