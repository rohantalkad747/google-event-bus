package com.trident.load_balancer;

public interface Node {

    int getConnections();

    String getHostName();

    boolean isActive();

    double getPercentUsage(Component component);
}
