package com.trident.load_balancer;

public class HeartbeatMediator {
    private final EventBus eventBus = new EventBus("HeartbeatMediator");

    public HeartbeatMediator(Cluster clusterHalfSecondHb) {


    }

    public void onNewHb(String ipAddress, Heartbeat valid) {

    }
}
