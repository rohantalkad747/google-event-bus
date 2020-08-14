package com.trident.load_balancer;

import com.google.common.collect.Maps;
import lombok.Getter;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class Cluster {
    @Getter
    private final int heartbeatPeriodMs;
    private final Map<InetAddress, Node> nodes = Maps.newHashMap();

    public Cluster(int heartbeatPeriodMs) {
        this.heartbeatPeriodMs = heartbeatPeriodMs;
    }

    public void addNode(Node node) {
        InetAddress ipAddress = node.getIpAddress();
        nodes.put(ipAddress, node);
    }

    public boolean exists(InetAddress ipAddress) {
        return nodes
                .values()
                .stream()
                .map(Node::getIpAddress)
                .anyMatch(ipAddress::equals);
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

    public Optional<Node> getNode(InetAddress ipAddress) {
        return Optional.ofNullable(nodes.get(ipAddress));
    }
}
