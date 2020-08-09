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

    public void addNode(Node node) {
        nodes.add(node);
    }

    public void removeNode(Node node) {
        nodes.remove(node);
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
                .filter(n -> n.getUri().equals(uri))
                .findFirst();
    }

}
