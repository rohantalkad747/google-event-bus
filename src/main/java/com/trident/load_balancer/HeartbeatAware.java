package com.trident.load_balancer;

public interface HeartbeatAware {
    void onHeartbeat(Heartbeat heartbeat);
}
