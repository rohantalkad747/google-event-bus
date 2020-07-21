package com.trident.load_balancer;

import java.util.List;

/**
 * Interface for load balancing strategies.
 */
public interface BalancingStrategy {
    /**
     * @return a virtual machine best suited for the next request.
     */
    Node getVMTarget(List<Node> vms);
}
