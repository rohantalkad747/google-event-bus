package com.trident.load_balancer;

import com.google.common.collect.Maps;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.net.URI;
import java.util.Map;

@AllArgsConstructor
@Data
public class VirtualMachine implements Node, HeartbeatAware {
    private URI uri;
    private boolean isActive;
    private int connections;
    private double ramUsage;
    private double cpuUsage;

    VirtualMachine(URI uri) {
        this.uri = uri;
    }

    @Override
    public String getHostName() {
        return uri.getHost();
    }

    @Override
    public boolean isActive() {
        return isActive;
    }

    public void updateConnections(int updatedConnections) {
        connections = updatedConnections;
    }

    @Override
    public void onHeartbeat(Heartbeat heartbeat) {
        updateComponentsUsage(heartbeat);
        updateConnections(heartbeat.getConnections());
    }

    private void updateComponentsUsage(Heartbeat heartbeat) {
        usageStats.put(Component.CPU, heartbeat.getCpuUsage());
        usageStats.put(Component.RAM, heartbeat.getRamUsage());
    }
}
