package com.trident.load_balancer;

import com.google.common.collect.Lists;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class Cluster {
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

    public Optional<Node> getNode(URI uri) {
        return nodes
                .stream()
                .filter((Node n) -> n.getHostName().equals(uri.getHost()))
                .findFirst();
    }

}
