package com.trident.load_balancer;

import com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;

import java.util.Map.Entry;

import static com.trident.load_balancer.Component.*;

@Slf4j
public class NodeComponentUpdateReceiver {

    private final Cluster cluster;

    public NodeComponentUpdateReceiver(final Cluster cluster) {
        this.cluster = cluster;
    }

    @Subscribe
    public void onHeartbeat(Heartbeat heartbeat) {
        ImmutableMap<Component, Number> componentUsage = getValidComponentsForUpdate(heartbeat);
        dispatchHbToNode(componentUsage, heartbeat.getIpAddress());
    }

    private void dispatchHbToNode(ImmutableMap<Component, Number> componentUsage, String nodeUri) {
        Node node = cluster.getNode(nodeUri);
        log.info(String.format("Updating the node %s with new component usage data %s", node, componentUsage));
        for (Entry<Component, Number> componentUsageEntry : componentUsage.entrySet()) {
            node.updateComponentUsage(componentUsageEntry.getKey(), componentUsageEntry.getValue());
        }
    }

    private ImmutableMap<Component, Number> getValidComponentsForUpdate(Heartbeat hb) {
        ImmutableMap.Builder<Component, Number> components = ImmutableMap.builder();
        Double cpuUsage = hb.getCpuUsage();
        Double ramUsage = hb.getRamUsage();
        Integer connections = hb.getConnections();
        if (isPercentage(cpuUsage)) {
            components.put(CPU, cpuUsage);
        }
        if (isPercentage(ramUsage)) {
            components.put(RAM, ramUsage);
        }
        if (isPositive(connections)) {
            components.put(CONNECTIONS, connections);
        }
        return components.build();
    }

    private boolean isPositive(Integer connections) {
        return connections != null && connections > 0;
    }

    private boolean isPercentage(Double val) {
        return val != null && val >= 0 && val <= 1;
    }

}
