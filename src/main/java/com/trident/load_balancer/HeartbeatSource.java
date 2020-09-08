package com.trident.load_balancer;

import java.util.concurrent.atomic.AtomicReference;

public class HeartbeatSource {
    private final AtomicReference<Heartbeat> hbReference = new AtomicReference<>();

    public HeartbeatSource() {
        hbReference.set(new Heartbeat(new ComponentUsageService()));
    }

    public Heartbeat beat() {
        Heartbeat previousHeartbeat, nextHeartbeat;
        do {
            previousHeartbeat = hbReference.get();
            nextHeartbeat = previousHeartbeat.nextHeartbeat();
        } while (!hbReference.compareAndSet(previousHeartbeat, nextHeartbeat));
        return nextHeartbeat;
    }
}
