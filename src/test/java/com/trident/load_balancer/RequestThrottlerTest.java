package com.trident.load_balancer;


import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class RequestThrottlerTest {

    @Test
    void whenMoreThanMaxRequestsThrottlerShouldFalseOnNewRequest() {
        // Given limit of 100 reqs per 100 ms
        final int maxRequestsPerTimeUnit = 100;
        RequestThrottler requestThrottler = new RequestThrottler(maxRequestsPerTimeUnit, TimeUnit.MILLISECONDS, 100);
        requestThrottler.start();
        long startTime = System.currentTimeMillis();
        // When we do 99 requests
        for (int i = 0; i < maxRequestsPerTimeUnit; i++) {
            // Then all these requests should be able to proceed
            assertTrue(requestThrottler.canProceed());
        }
        //      And on the 101th request we should not
        assertFalse(requestThrottler.canProceed());
        //      And when the new time unit cycle starts, we should be able to proceed
        waitUntilThrottleTimeOver(startTime + 150);
        assertTrue(requestThrottler.canProceed());
    }

    private void waitUntilThrottleTimeOver(long endTime) {
        while (System.currentTimeMillis() <= endTime) {
        }
    }
}