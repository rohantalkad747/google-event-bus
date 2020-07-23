package com.trident.load_balancer;

import java.net.Socket;
import java.util.function.Predicate;

public interface Node {

    Socket getSocket();

    int getConnections();

    boolean isActive();

    double getPercentUsage(Component component);
}
