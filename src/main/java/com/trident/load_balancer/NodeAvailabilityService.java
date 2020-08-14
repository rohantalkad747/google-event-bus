package com.trident.load_balancer;

import com.google.common.collect.ImmutableSet;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;

import java.net.InetAddress;
import java.time.Duration;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class NodeAvailabilityService {
    private final WebClient webClient = WebClient.create();

    private final Cluster cluster;

    // Expect to hear from nodes within ~2x the heartbeat period
    private final int heartbeatPeriodThresholdMs;

    // Store last heartbeat timestamps
    private final Map<Node, Long> heartbeatLog;

    // Will consider node dead if ping time threshold is broken
    private final int DEAD_NODE_PING_CHECK_THRESHOLD_SECONDS = 5;

    public NodeAvailabilityService(Map<Node, Long> heartbeatLog, Cluster cluster) {
        this.heartbeatLog = heartbeatLog;
        this.cluster = cluster;
        this.heartbeatPeriodThresholdMs = 2 * cluster.getHeartbeatPeriodMs();
        spawnDeadNodePollingTask();
    }

    private void spawnDeadNodePollingTask() {
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        scheduledExecutorService.scheduleAtFixedRate(
                checkForDeadNodes(),
                heartbeatPeriodThresholdMs,
                heartbeatPeriodThresholdMs,
                TimeUnit.MILLISECONDS
        );
    }

    private Runnable checkForDeadNodes() {
        return () -> {
            long currentTimestamp = System.currentTimeMillis() - heartbeatPeriodThresholdMs;
            ImmutableSet<Node> pingCandidates = checkForMaybeDeadNodes(currentTimestamp);
            if (!pingCandidates.isEmpty()) {
                tryPing(pingCandidates);
            }
        };
    }

    private ImmutableSet<Node> checkForMaybeDeadNodes(long currentTimestamp) {
        Iterator<Map.Entry<Node, Long>> iterator = heartbeatLog.entrySet().iterator();
        ImmutableSet.Builder<Node> maybeDeadNodesBuilder = ImmutableSet.builder();
        while (iterator.hasNext()) {
            Map.Entry<Node, Long> entry = iterator.next();
            if (entry.getValue() <= currentTimestamp) {
                Node node = entry.getKey();
                maybeDeadNodesBuilder.add(node);
            }
        }
        return maybeDeadNodesBuilder.build();
    }

    private void tryPing(ImmutableSet<Node> pingCandidates) {
        pingCandidates
                .stream()
                .parallel()
                .forEach(handlePingFailure());
    }

    @NonNull
    private Consumer<Node> handlePingFailure() {
        return node -> {
            String pingCheckUrl = node.getIpAddress() + "/ping";
            try {
                if (!success(doDeadNodePing(pingCheckUrl))) {
                    node.setActive(false);
                }
            } catch (RuntimeException e) {
                if (e.getMessage().startsWith("Timeout on blocking read for")) {
                    node.setActive(false);
                }
            }
        };
    }

    private boolean success(ClientResponse clientResponse) {
        return clientResponse.statusCode().is2xxSuccessful();
    }

    private ClientResponse doDeadNodePing(String pingCheck) {
        return webClient.get()
                .uri(pingCheck)
                .exchange()
                .block(Duration.ofSeconds(DEAD_NODE_PING_CHECK_THRESHOLD_SECONDS));
    }

    public void onNewHeartbeat(InetAddress ipAddress, Heartbeat heartbeat) {
        Optional<Node> maybeNode = cluster.getNode(ipAddress);
        maybeNode.ifPresentOrElse(node -> {
            if (!node.isActive()) {
                node.setActive(true);
            }
        }, RuntimeException::new);
    }
}
