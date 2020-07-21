package com.trident.load_balancer;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Cluster implements VMAware {
    private final List<VirtualMachine> virtualMachines;

    public Cluster() {
        virtualMachines = new ArrayList<>();
    }

    public Cluster(List<VirtualMachine> virtualMachines) {
        this.virtualMachines = virtualMachines;
    }

    @Override
    public void addVM(VirtualMachine virtualMachine) {
        virtualMachines.add(virtualMachine);
    }

    @Override
    public void removeVM(VirtualMachine virtualMachine) {
        virtualMachines.remove(virtualMachine);
    }

    public List<Node> getAvailableNodes() {
        return virtualMachines.stream().filter(VirtualMachine::isActive).collect(Collectors.toList());
    }
}
