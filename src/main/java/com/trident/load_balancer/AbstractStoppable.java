package com.trident.load_balancer;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractStoppable {
    private final AtomicBoolean alive = new AtomicBoolean();

    public void stop() {
        if (alive.get())
            alive.set(false);
        throw new RuntimeException("Already stopped!");
    }

    protected boolean isActive() {
        return alive.get();
    }
}
