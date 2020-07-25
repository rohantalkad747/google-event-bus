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
