package com.trident.load_balancer;

import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
class LoadBalancer {
    /**
     * The computing cluster to balance network load.
     */
    private Cluster cluster;

    /**
     * The strategy for balancing network load.
     */
    private BalancingStrategy balancingStrategy;

    Node getNextAvailableHost() {
        List<Node> availableVMs = cluster.getAvailableNodes();
        if (!availableVMs.isEmpty()) {
            return balancingStrategy.getVMTarget(availableVMs);
        }
        throw new NoHostAvailableException();
    }


    static class NoHostAvailableException extends RuntimeException {
    }
}
