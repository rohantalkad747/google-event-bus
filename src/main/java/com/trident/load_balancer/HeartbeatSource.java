package com.trident.load_balancer;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

public class HeartbeatSource {
    private final AtomicReference<Heartbeat> hbReference = new AtomicReference<>();

    public HeartbeatSource() {
        hbReference.set(new Heartbeat());
    }

    public Heartbeat beat() throws IOException, InterruptedException {
        Heartbeat previousHeartbeat, nextHeartbeat;
        do {
            previousHeartbeat = hbReference.get();
            nextHeartbeat = previousHeartbeat.nextHeartbeat();
        } while (!hbReference.compareAndSet(previousHeartbeat, nextHeartbeat));
        return nextHeartbeat;
    }
}
