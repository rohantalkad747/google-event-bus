package com.trident.load_balancer;

import com.google.common.base.Preconditions;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

class RequestThrottler {
    private final ScheduledExecutorService scheduler;

    private final AtomicInteger requestsPerTimeUnit;

    private final int maxRequestsPerTimeUnit;

    private final TimeUnit throttlingCheckTimeUnit;

    private final int throttlingCheckTimeAmount;

    RequestThrottler(int maxRequestsPerTimeUnit, TimeUnit throttlingCheckTimeUnit, int throttlingCheckTimeAmount) {
        Preconditions.checkState(maxRequestsPerTimeUnit > 0, "Max requests must be positive");
        Preconditions.checkState(throttlingCheckTimeAmount > 0, "Throttling time must be positive");

        this.maxRequestsPerTimeUnit = maxRequestsPerTimeUnit;
        this.requestsPerTimeUnit = new AtomicInteger();
        this.throttlingCheckTimeUnit = throttlingCheckTimeUnit;
        this.throttlingCheckTimeAmount = throttlingCheckTimeAmount;
        this.scheduler = Executors.newScheduledThreadPool(1);

        this.initRequestCountResetTask();
    }

    private void initRequestCountResetTask() {
        Runnable resetRequestCountTask = () -> requestsPerTimeUnit.set(0);
        scheduler.scheduleAtFixedRate(resetRequestCountTask, 0, throttlingCheckTimeAmount, throttlingCheckTimeUnit);
    }

    boolean canProceed() {
        return requestsPerTimeUnit.incrementAndGet() <= maxRequestsPerTimeUnit;
    }
}
