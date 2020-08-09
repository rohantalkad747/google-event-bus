package com.trident.load_balancer;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public enum BalancingStrategies implements BalancingStrategy {
    NETWORK_OPTIMIZED {
        @Override
        public Node getVMTarget(List<Node> vms) {
            return getLowestValuedVMBasedOnComparator(Comparator.comparing(Node::getConnections), vms);
        }
    },
    MEMORY_OPTIMIZED {
        @Override
        public Node getVMTarget(List<Node> vms) {
            return getLowestValuedVMBasedOnComparator(Comparator.comparing(Node::getRamUsage), vms);
        }
    },
    CPU_OPTIMIZED {
        @Override
        public Node getVMTarget(List<Node> vms) {
            return getLowestValuedVMBasedOnComparator(Comparator.comparing(Node::getCpuUsage), vms);
        }
    },
    DYNAMIC_BALANCED {

        final Comparator<Node> balancedComparator = Comparator.comparing(Node::getCpuUsage)
                .thenComparing(Node::getRamUsage)
                .thenComparing(Node::getConnections);

        @Override
        public Node getVMTarget(List<Node> vms) {
            return getLowestValuedVMBasedOnComparator(balancedComparator, vms);
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
