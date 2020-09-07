package com.trident.load_balancer;

import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

@Slf4j
public class HeartbeatMediator {
    private final EventBus eventBus;

    private final Cluster cluster;

    private final Map<Node, Long> lastHeartbeat = Maps.newHashMap();

    public HeartbeatMediator(EventBus eventBus, Cluster cluster) {
        this.eventBus = eventBus;
        this.cluster = cluster;
    }

    public void onHeartbeat(Heartbeat heartbeat) {
        Node node = cluster.getNode(heartbeat.getIpAddress());
        Long lastTs = lastHeartbeat.get(node);
        if (heartbeat.getTimeEpochMs() > lastTs) {
            eventBus.post(heartbeat);
            lastHeartbeat.put(node, heartbeat.getTimeEpochMs());
        } else {
            throw new RuntimeException(
                    String.format(
                            "Heartbeat (timestamp=%s) is out of sync with the most latest processed heartbeat" +
                                    " processed heartbeat (timestamp=%s) for node with ip %s!",
                            heartbeat.getTimeEpochMs(),
                            lastTs,
                            heartbeat.getIpAddress()
                    )
            );
        }
    }
}
