package com.trident.load_balancer;

public enum Component {
    CPU {
        @Override
        double getPercentUsage() {
            return machine.getOsMXBean().getSystemLoadAverage() / machine.getOsMXBean().getAvailableProcessors();
        }
    },
    RAM {
        @Override
        double getPercentUsage() {
            return 1 - ((double) machine.getOsMXBean().getFreeMemorySize() / machine.getOsMXBean().getTotalMemorySize());
        }
    };
    VirtualMachine machine;

    abstract double getPercentUsage();
}
