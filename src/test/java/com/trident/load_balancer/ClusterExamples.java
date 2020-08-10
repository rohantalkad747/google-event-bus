package com.trident.load_balancer;

public class ClusterExamples {
    public static final Cluster SMALL_CLUSTER;

    static {
        SMALL_CLUSTER = new Cluster();
        SMALL_CLUSTER.addNode(NodeExamples.LOCAL_HOST_HB_30);
        SMALL_CLUSTER.addNode(NodeExamples.REMOTE_HB_60);
    }
}
