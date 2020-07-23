package com.trident.load_balancer;

import com.google.common.collect.Maps;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.net.Socket;
import java.util.Map;
import java.util.function.Predicate;

@AllArgsConstructor
public class VirtualMachine implements Node, ModifiableNode, HeartbeatAware {

    private final Map<Component, Double> usageStats;
    @Setter
    @Getter
    private Socket socket;
    @Setter
    private boolean isActive;
    @Getter
    private int connections;

    @Override
    public boolean isActive() {
        return isActive;
    }

    @Override
    public void updateConnections(int updatedConnections) {
        connections = updatedConnections;
    }

    @Override
    public double getPercentUsage(Component component) {
        return usageStats.get(component);
    }

    VirtualMachine(Socket socket) {
        this.socket = socket;
        this.usageStats = Maps.newHashMap();
    }
}
