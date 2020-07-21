package com.trident.load_balancer;

import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.trident.load_balancer.VirtualMachine.Component.*;

public enum BalancingStrategies implements BalancingStrategy {
    DISK_OPTIMIZED {
        @Override
        public Node getVMTarget(List<Node> vms) {
            return getLowestValuedVMBasedOnComparator(Comparator.comparing(vm -> vm.getPercentUsage(DISK)), vms);
        }
    },
    MEMORY_OPTIMIZED {
        @Override
        public Node getVMTarget(List<Node> vms) {
            return getLowestValuedVMBasedOnComparator(Comparator.comparing(vm -> vm.getPercentUsage(RAM)), vms);
        }
    },
    CPU_OPTIMIZED {
        @Override
        public Node getVMTarget(List<Node> vms) {
            return getLowestValuedVMBasedOnComparator(Comparator.comparing(vm -> vm.getPercentUsage(CPU)), vms);
        }
    },
    DYNAMIC_BALANCED {

        final Set<VirtualMachine.Component> componentSet = EnumSet.of(CPU, RAM, DISK);
        final Comparator<Node> balancedComparator = initVMStatsComparator(componentSet);

        @Override
        public Node getVMTarget(List<Node> vms) {
            return getLowestValuedVMBasedOnComparator(balancedComparator, vms);
        }

        Comparator<Node> initVMStatsComparator(final Set<VirtualMachine.Component> componentSet) {
            Comparator<Node> comparator = null;
            for (VirtualMachine.Component component : componentSet) {
                if (comparator == null) {
                    comparator = Comparator.comparing((Node vm) -> vm.getPercentUsage(component));
                } else {
                    comparator = comparator.thenComparing((Node vm) -> vm.getPercentUsage(component));
                }
            }
            return comparator;
        }
    },
    ROUND_ROBIN {

        final AtomicInteger vmTarget = new AtomicInteger();

        @Override
        public Node getVMTarget(List<Node> vms) {
            return vms.get(vmTarget.getAndIncrement() % vms.size());
        }
    };

    public abstract Node getVMTarget(List<Node> vms);

    Node getLowestValuedVMBasedOnComparator(Comparator<Node> comparator, List<Node> vms) {
        return vms
                .stream()
                .min(comparator)
                .orElseThrow(RuntimeException::new);
    }


}
