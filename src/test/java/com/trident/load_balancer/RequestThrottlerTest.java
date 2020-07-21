package com.trident.load_balancer;


import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RequestThrottlerTest {

    @Test
    public void whenMoreThanMaxRequestsThrottlerShouldFalseOnNewRequest() {
        // Given
        final int maxRequestsPerTimeUnit = 100;
        RequestThrottler requestThrottler = new RequestThrottler(maxRequestsPerTimeUnit, TimeUnit.MILLISECONDS, 10);
        for (int j = 0; j < 10; j++) {
            long startTime = System.currentTimeMillis();
            // When
            for (int i = 0; i < maxRequestsPerTimeUnit; i++) {
                // Then
                assertTrue(requestThrottler.canProceed());
            }
            assertFalse(requestThrottler.canProceed());
            waitUntilThrottleTimeOver(startTime, 10);
        }
    }

    private void waitUntilThrottleTimeOver(long startTime, long waitTime) {
        long endTime = startTime + waitTime;
        while (System.currentTimeMillis() <= endTime);
    }
}