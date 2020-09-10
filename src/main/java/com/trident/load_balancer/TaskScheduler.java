package com.trident.load_balancer;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class TaskScheduler {
    private final AtomicReference<ScheduledExecutorService> executor = new AtomicReference<>(null);
    private final Runnable consumer;

    public TaskScheduler(Runnable runnable) {
        this.consumer = runnable;
    }

    public void start(long intervalMs) {
        if (executorInactive()) {
            executor.set(Executors.newSingleThreadScheduledExecutor());
        }
        doSchedule(intervalMs);
    }

    private void doSchedule(long intervalMs) {
        executor.get().scheduleAtFixedRate(consumer, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        if (executor.get() != null) {
            executor.get().shutdown();
        }
    }

    public void reset(int intervalMs) {
        stop();
        start(intervalMs);
    }

    public void reset(Runnable runnable, int intervalMs) {
        if (executorInactive()) {
            throw new RuntimeException();
        }
        doSchedule(intervalMs);
    }

    private boolean executorInactive() {
        return executor.get() == null || executor.get().isShutdown() || executor.get().isTerminated();
    }
}
