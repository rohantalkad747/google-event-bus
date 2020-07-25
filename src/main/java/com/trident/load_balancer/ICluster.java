package com.trident.load_balancer;

public interface ICluster {
    void addNode(VirtualMachine virtualMachine);

    void removeNode(VirtualMachine virtualMachine);
}
