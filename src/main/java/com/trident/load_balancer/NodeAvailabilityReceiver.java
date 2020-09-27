package com.trident.load_balancer;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

@Slf4j
public class NodeAvailabilityReceiver {
    // Will consider node dead if ping time threshold is broken
    private final Duration pingWaitTime;
    private final WebClient webClient;
    private final Cluster cluster;
    // Expect to hear from nodes within ~2x the heartbeat period
    private final int heartbeatPeriodThresholdMs;
    // Store last heartbeat timestamps
    private final Map<Node, Long> heartbeatLog;
    private final long initTime = System.currentTimeMillis();

    public class PingService {
        boolean doPing(String pingCheckUrl) {
            return Objects.requireNonNull(webClient.get()
                    .uri(pingCheckUrl)
                    .exchange()
                    .block(pingWaitTime))
                    .statusCode().is2xxSuccessful();

        }
    }

    public NodeAvailabilityReceiver(Cluster cluster, WebClient webClient, Duration pingWaitTime, Map<Node, Long> heartbeatLog) {
        this.pingWaitTime = pingWaitTime;
        this.webClient = webClient;
        this.cluster = cluster;
        this.heartbeatLog = heartbeatLog;
        this.heartbeatPeriodThresholdMs = 2 * cluster.getHeartbeatPeriodMs();
        spawnDeadNodePollingTask();
    }

    private void spawnDeadNodePollingTask() {
        new TaskScheduler(checkForDeadNodes()).start(heartbeatPeriodThresholdMs);
    }

    private Runnable checkForDeadNodes() {
        return () -> {
            long currentTimestamp = System.currentTimeMillis() - heartbeatPeriodThresholdMs;
            ImmutableCollection<Node> potentiallyDeadNodes = getPotentiallyDeadNodes(currentTimestamp);
            if (!potentiallyDeadNodes.isEmpty()) {
                tryPingingPotentiallyDeadNodes(potentiallyDeadNodes);
            }
        };
    }

    private ImmutableCollection<Node> getPotentiallyDeadNodes(long currentTimestamp) {
        List<Node> nodes = cluster.getAvailableNodes();
        ImmutableList.Builder<Node> maybeDeadNodesBuilder = ImmutableList.builder();
        boolean initPingTime = System.currentTimeMillis() > initTime + heartbeatPeriodThresholdMs;
        for (Node node : nodes) {
            Long lastHeartbeat = heartbeatLog.get(node);
            if (initPingTime || (lastHeartbeat != null && lastHeartbeat <= currentTimestamp)) {
                maybeDeadNodesBuilder.add(node);
            }
        }
        return maybeDeadNodesBuilder.build();
    }

    private void tryPingingPotentiallyDeadNodes(ImmutableCollection<Node> pingCandidates) {
        pingCandidates
                .stream()
                .parallel()
                .forEach(handlePingFailure());
    }

    private Consumer<Node> handlePingFailure() {
        return node -> {
            String pingCheckUrl = node.getHostName() + "/ping";
            boolean timeoutRuntimeThrown = false, pingSuccess = false;
            try {
                log.info(String.format("Pinging node [%s] to see if it's alive", node.getHostName()));
                ClientResponse clientResponse = doDeadNodePing(pingCheckUrl);
                pingSuccess = success(clientResponse);
            } catch (RuntimeException e) {
                log.info(String.format("Timeout while pinging node [%s]", node.getHostName()));
                timeoutRuntimeThrown = e.getMessage().startsWith("Timeout on blocking read for");
            } finally {
                checkIfShouldDeactivateNode(node, timeoutRuntimeThrown, pingSuccess);
            }
        };
    }

    private void checkIfShouldDeactivateNode(Node node, boolean timeoutRuntimeThrown, boolean pingSuccess) {
        if (timeoutRuntimeThrown || !pingSuccess) {
            log.info(String.format("Setting node to be inactive: %s", node));
            node.setActive(false);
        } else {
            log.info(String.format("Node [%s] is alive", node.getHostName()));
        }
    }


    private boolean success(ClientResponse clientResponse) {
        return clientResponse.statusCode().is2xxSuccessful();
    }

    private ClientResponse doDeadNodePing(String pingCheck) {
        return webClient.get()
                .uri(pingCheck)
                .exchange()
                .block(pingWaitTime);
    }

    @Subscribe
    public void onNewHeartbeat(Heartbeat heartbeat) {
        Node node = cluster.getNode(heartbeat.getIpAddress());
        if (!node.isActive()) {
            node.setActive(true);
        }
    }
}
