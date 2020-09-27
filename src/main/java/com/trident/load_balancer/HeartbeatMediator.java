package com.trident.load_balancer;

import lombok.extern.slf4j.Slf4j;

import java.util.Map;

@Slf4j
public class HeartbeatMediator {
    private final EventBus eventBus;

    private final Cluster cluster;

    private final Map<Node, Long> lastHeartbeat;

    public HeartbeatMediator(EventBus eventBus, Cluster cluster, Map<Node, Long> lastHeartbeat) {
        this.eventBus = eventBus;
        this.cluster = cluster;
        this.lastHeartbeat = lastHeartbeat;
    }

    public void onHeartbeat(Heartbeat heartbeat) {
        Node node = cluster.getNode(heartbeat.getIpAddress());
        Long lastTs = lastHeartbeat.get(node);
        if (lastTs == null || heartbeat.getTimeEpochMs() >= lastTs) {
            lastHeartbeat.put(node, heartbeat.getTimeEpochMs());
            eventBus.post(heartbeat);
        } else {
            log.warn(String.format(
                    "Heartbeat (timestamp=%s) is out of sync with the most latest processed heartbeat" +
                            " processed heartbeat (timestamp=%s) for node with ip %s!",
                    heartbeat.getTimeEpochMs(),
                    lastTs,
                    heartbeat.getIpAddress()
            ));
        }
    }

    public Long getLatestHeartbeatTimestampFromNode(String ipAddr) {
        return lastHeartbeat
                .entrySet()
                .stream()
                .peek(System.out::println)
                .filter(kv -> kv.getKey().getHostName().equals(ipAddr))
                .findFirst()
                .map(Map.Entry::getValue)
                .orElse((long) -1);
    }
}
