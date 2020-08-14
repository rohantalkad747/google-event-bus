package com.trident.load_balancer;

import java.time.Duration;

public class ClusterExamples {
    public static final Cluster CLUSTER_HALF_SECOND_HB;

    static {
        CLUSTER_HALF_SECOND_HB = new Cluster(Duration.ofMillis(500));
        CLUSTER_HALF_SECOND_HB.addNode(NodeExamples.LOCAL_HOST);
        CLUSTER_HALF_SECOND_HB.addNode(NodeExamples.REMOTE);
    }
}
