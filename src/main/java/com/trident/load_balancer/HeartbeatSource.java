package com.trident.load_balancer;

import lombok.Getter;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

public class HeartbeatSource {
    private final AtomicReference<Heartbeat> hbReference = new AtomicReference<>();
    @Getter
    private final int beatPeriod;

    public HeartbeatSource(int beatPeriod) {
        this.beatPeriod = beatPeriod;
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
