package com.trident.load_balancer;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractStoppable {
    private final AtomicBoolean alive = new AtomicBoolean(true);

    public void stop() {
        if (alive.get())
            alive.set(false);
        else
            throw new RuntimeException("Already stopped!");
    }

    protected boolean isActive() {
        return alive.get();
    }
}
