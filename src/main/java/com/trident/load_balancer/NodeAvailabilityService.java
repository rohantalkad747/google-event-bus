package com.trident.load_balancer;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.Duration;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

@Slf4j
public class NodeAvailabilityService {
    // Will consider node dead if ping time threshold is broken
    private static final int DEAD_NODE_PING_CHECK_THRESHOLD_SECONDS = 5;
    private final WebClient webClient;
    private final Cluster cluster;
    // Expect to hear from nodes within ~2x the heartbeat period
    private final int heartbeatPeriodThresholdMs;
    // Store last heartbeat timestamps
    private final Map<Node, Long> heartbeatLog = Maps.newHashMap();

    public NodeAvailabilityService(Cluster cluster, WebClient webClient) {
        this.webClient = webClient;
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
            ImmutableCollection<Node> potentiallyDeadNodes = getPotentiallyDeadNodes(currentTimestamp);
            if (!potentiallyDeadNodes.isEmpty()) {
                tryPingingPotentiallyDeadNodes(potentiallyDeadNodes);
            }
        };
    }

    private ImmutableCollection<Node> getPotentiallyDeadNodes(long currentTimestamp) {
        Iterator<Map.Entry<Node, Long>> iterator = heartbeatLog.entrySet().iterator();
        ImmutableList.Builder<Node> maybeDeadNodesBuilder = ImmutableList.builder();
        while (iterator.hasNext()) {
            Map.Entry<Node, Long> entry = iterator.next();
            if (entry.getValue() <= currentTimestamp) {
                Node node = entry.getKey();
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

    @NonNull
    private Consumer<Node> handlePingFailure() {
        return node -> {
            String pingCheckUrl = node.getIpAddress() + "/ping";
            boolean timeoutRuntimeThrown = false, pingSuccess = false;
            try {
                log.info(String.format("Pinging node [%s] to see if it's alive"), node.getIpAddress());
                ClientResponse clientResponse = doDeadNodePing(pingCheckUrl);
                pingSuccess = success(clientResponse);
            } catch (RuntimeException e) {
                timeoutRuntimeThrown = e.getMessage().startsWith("Timeout on blocking read for");
            } finally {
                if (timeoutRuntimeThrown || !pingSuccess) {
                    setInactive(node);
                } else {
                    log.info(String.format("Node [%s] is alive", node.getIpAddress()));
                }
            }
        };
    }

    private void setInactive(Node node) {
        log.info(
                String.format("Node %s is being set to inactive", node)
        );
        node.setActive(false);
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

    public void onNewHeartbeat(String ipAddress, Heartbeat heartbeat) {
        Optional<Node> maybeNode = cluster.getNode(ipAddress);
        maybeNode.ifPresentOrElse(node -> {
            if (!node.isActive()) {
                node.setActive(true);
            }
        }, RuntimeException::new);
    }
}
