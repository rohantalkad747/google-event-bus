package com.trident.load_balancer;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class Cluster  {
    private final List<Node> nodes;

    public Cluster() {
        nodes = Lists.newArrayList();
    }

    public void addNode(VirtualMachine virtualMachine) {
        nodes.add(virtualMachine);
    }

    public void removeNode(VirtualMachine virtualMachine) {
        nodes.remove(virtualMachine);
    }

    public List<Node> getAvailableNodes() {
        return nodes
                .stream()
                .filter(Node::isActive)
                .collect(Collectors.toList());
    }

    public Optional<Node> getNode(String ipAddress, int port) {
        return nodes
                .stream()
                .filter(forPort(port))
                .filter(forIpAddress(ipAddress))
                .findFirst();
    }

    private Predicate<Node> forIpAddress(String ipAddress) {
        return n -> n.getSocket().getInetAddress().getHostAddress().equals(ipAddress);
    }

    private Predicate<Node> forPort(int port) {
        return n -> n.getSocket().getPort() == port;
    }
}
