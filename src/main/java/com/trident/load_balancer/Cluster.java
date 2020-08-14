package com.trident.load_balancer;

import com.google.common.collect.Maps;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@NoArgsConstructor
public class Cluster {
    private final Map<String, Node> nodes = Maps.newHashMap();
    @Getter
    private int heartbeatPeriodMs = 1000;

    public Cluster(Duration duration) {
        this.heartbeatPeriodMs = (int) duration.toMillis();
    }

    public void addNode(Node node) {
        String ipAddress = node.getIpAddress();
        nodes.put(ipAddress, node);
    }

    public boolean exists(String ipAddress) {
        return nodes.containsKey(ipAddress);
    }

    public void removeNode(Node node) {
        nodes.remove(node.getIpAddress());
    }

    public List<Node> getAvailableNodes() {
        return nodes
                .values()
                .stream()
                .filter(Node::isActive)
                .collect(Collectors.toList());
    }

    public Optional<Node> getNode(String ipAddress) {
        return Optional.ofNullable(nodes.get(ipAddress));
    }
}
