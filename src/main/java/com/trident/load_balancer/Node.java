package com.trident.load_balancer;

import java.net.URI;

public interface Node {

    int getConnections();

    String getHostName();

    boolean isActive();

    double getPercentUsage(Component component);

    void setUri(URI uri);

    void updateConnections(int connections);
}
