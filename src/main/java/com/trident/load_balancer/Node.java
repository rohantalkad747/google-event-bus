package com.trident.load_balancer;

public interface Node {
    String getHostName();
    double getPercentUsage(VirtualMachine.Component component);
}
