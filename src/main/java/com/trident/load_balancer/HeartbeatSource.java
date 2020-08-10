package com.trident.load_balancer;

import lombok.Getter;

import java.util.concurrent.atomic.AtomicReference;

public class HeartbeatSource extends AbstractStoppable{
    private final AtomicReference<Heartbeat> hbReference = new AtomicReference<>();
    @Getter
    private final int beatPeriod;

    public HeartbeatSource(int beatPeriod) {
        this.beatPeriod = beatPeriod;
        hbReference.set(new Heartbeat());
    }

    public Heartbeat beat() {
        Heartbeat previousHeartbeat, nextHeartbeat;
        do {
            previousHeartbeat = hbReference.get();
            nextHeartbeat = previousHeartbeat.nextHeartbeat();
        } while (isActive() && !hbReference.compareAndSet(previousHeartbeat, nextHeartbeat));
        return nextHeartbeat;
    }
}
