package com.trident.load_balancer;

public interface VMAware {
    void addVM(VirtualMachine virtualMachine);

    void removeVM(VirtualMachine virtualMachine);
}
