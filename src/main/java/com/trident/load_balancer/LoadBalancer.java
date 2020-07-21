package com.trident.load_balancer;

import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Data
@Builder
@NoArgsConstructor
class LoadBalancer {

    private Cluster cluster;

    private AtomicInteger vmTarget;

    private BalancingStrategy balancingStrategy;

    String getNextAvailableHost() {
        List<Node> availableVMs = cluster.getAvailableNodes();
        if (!availableVMs.isEmpty()) {
            Node vmTarget = balancingStrategy.getVMTarget(availableVMs);
            return vmTarget.getHostName();
        }
        throw new NoHostAvailableException();
    }


    class NoHostAvailableException extends RuntimeException {
    }
}
