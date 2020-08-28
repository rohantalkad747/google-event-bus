package com.trident.load_balancer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@NoArgsConstructor
public class Cluster
{
    private final Map<String, Node> nodes = Maps.newHashMap();
    @Getter
    private int heartbeatPeriodMs = 1000;

    public Cluster(Duration duration)
    {
        this.heartbeatPeriodMs = (int) duration.toMillis();
    }

    public void addNode(Node node)
    {
        String ipAddress = node.getHostName();
        nodes.put(ipAddress, node);
        new Thread();
    }

    public boolean exists(String ipAddress)
    {
        return nodes.containsKey(ipAddress);
    }

    public void removeNode(Node node)
    {
        nodes.remove(node.getHostName());
    }

    public ImmutableList<Node> getNodes()
    {
        return ImmutableList.copyOf(nodes.values());
    }

    public List<Node> getAvailableNodes()
    {
        return nodes
                .values()
                .stream()
                .filter(Node::isActive)
                .collect(Collectors.toList());
    }

    public Node getNode(String ipAddress)
    {
        return nodes.get(ipAddress);
    }
}
