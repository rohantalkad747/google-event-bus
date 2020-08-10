package com.trident.load_balancer;

public class NodeAvailabilityService {
    private final Cluster cluster;

    public NodeAvailabilityService(Cluster cluster) {
        this.cluster = cluster;
    }

    public void onNewHeartbeat(Heartbeat validHb) {

    }
}
