package com.trident.load_balancer;

import java.time.Duration;

public class ClusterExamples {
    public static Cluster CLUSTER_HALF_SECOND_HB;

    static {
        initCluster();
    }

    static void reset() {
        NodeExamples.reset();
        initCluster();
    }

    private static void initCluster() {
        CLUSTER_HALF_SECOND_HB = new Cluster(Duration.ofMillis(500));
        CLUSTER_HALF_SECOND_HB.addNode(NodeExamples.NODE_8383);
        CLUSTER_HALF_SECOND_HB.addNode(NodeExamples.NODE_8080);
    }


}
