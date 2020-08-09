package com.trident.load_balancer;

import lombok.Getter;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class HeartbeatSource {
    private final AtomicReference<Heartbeat> hbReference = new AtomicReference<>();
    private final AtomicBoolean alive = new AtomicBoolean(true);
    @Getter
    private final int beatPeriod;

    public HeartbeatSource(int beatPeriod) {
        this.beatPeriod = beatPeriod;
        hbReference.set(new Heartbeat());
    }

    public void kill() {
        alive.set(false);
    }

    public Heartbeat beat() {
        Heartbeat previousHeartbeat, nextHeartbeat;
        do {
            previousHeartbeat = hbReference.get();
            nextHeartbeat = previousHeartbeat.nextHeartbeat();
        } while (alive.get() && !hbReference.compareAndSet(previousHeartbeat, nextHeartbeat));
        return nextHeartbeat;
    }
}
