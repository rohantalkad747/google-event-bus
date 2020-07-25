package com.trident.load_balancer;

import com.google.common.collect.Maps;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.net.URI;
import java.util.Map;

@AllArgsConstructor
public class VirtualMachine implements Node, ModifiableNode, HeartbeatAware {

    private final Map<Component, Double> usageStats;
    @Setter
    @Getter
    private URI URI;
    @Setter
    private boolean isActive;
    @Getter
    private int connections;

    VirtualMachine(URI uri) {
        this.URI = uri;
        this.usageStats = Maps.newHashMap();
    }

    @Override
    public String getHostName() {
        return URI.getHost();
    }

    @Override
    public boolean isActive() {
        return isActive;
    }

    public void updateConnections(int updatedConnections) {
        connections = updatedConnections;
    }

    @Override
    public double getPercentUsage(Component component) {
        return usageStats.get(component);
    }

    @Override
    public void onHeartbeat(Heartbeat heartbeat) {
        updateComponentsUsage(heartbeat);
        updateConnections(heartbeat.getConnections());
    }

    private void updateComponentsUsage(Heartbeat heartbeat) {
        usageStats.put(Component.CPU, heartbeat.getCpuUsage());
        usageStats.put(Component.RAM, heartbeat.getRamUsage());
        usageStats.put(Component.NETWORK, heartbeat.getNetworkUsage());
    }
}
