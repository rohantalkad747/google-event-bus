package com.trident.load_balancer;

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class NodeAvailabilityServiceTest {
    private final NodeAvailabilityService nodeAvailabilityService = new NodeAvailabilityService(ClusterExamples.SMALL_CLUSTER);

    @Test
    void testHb() {
        assertTrue(NodeExamples.LOCAL_HOST_HB_30.isActive());
        Heartbeat validHb = HeartbeatMonitorTest.HeartbeatExamples.VALID;
        nodeAvailabilityService.onNewHeartbeat(NodeExamples.LOCAL_INET_ADDR, validHb);
        assertTrue(NodeExamples.LOCAL_HOST_HB_30.isActive());
    }

    @Test
    void testNoHbOnePeriod() throws InterruptedException {
        Heartbeat validHb = HeartbeatMonitorTest.HeartbeatExamples.VALID;
        nodeAvailabilityService.onNewHeartbeat(NodeExamples.LOCAL_INET_ADDR, validHb);
        // Given at least a 30 second break from the last heartbeat
        TimeUnit.MILLISECONDS.sleep(30);
        // When we ask if this node is active
        boolean isActive = NodeExamples.LOCAL_HOST_HB_30.isActive();
        // Then it should still be true since it's hasn't been 60 seconds
        assertTrue(isActive);
    }

    @Test
    void testNoHbTwoPeriods() throws InterruptedException {
        Heartbeat validHb = HeartbeatMonitorTest.HeartbeatExamples.VALID;
        // Given at least a 60 second break from initialization
        TimeUnit.MILLISECONDS.sleep(60);
        // When we ask if this node is active
        boolean isActive = NodeExamples.LOCAL_HOST_HB_30.isActive();
        // Then it should still be false since at least 2 periods passed with no heartbeat for this node
        assertFalse(isActive);
    }

    @Test
    void testMultipleInvalidNodes() throws InterruptedException {
        Heartbeat invalidHb = HeartbeatMonitorTest.HeartbeatExamples.WITH_INVALID_CPU_USAGE;
        nodeAvailabilityService.onNewHeartbeat(NodeExamples.LOCAL_INET_ADDR, invalidHb);
        TimeUnit.MILLISECONDS.sleep(30);
        nodeAvailabilityService.onNewHeartbeat(NodeExamples.LOCAL_INET_ADDR, invalidHb);
        TimeUnit.MILLISECONDS.sleep(60);
        boolean isActive = NodeExamples.LOCAL_HOST_HB_30.isActive();
        assertFalse(isActive);
    }

    @Test
    void testReavailability() throws InterruptedException {
        TimeUnit.MILLISECONDS.sleep(60);
        Heartbeat validHb = HeartbeatMonitorTest.HeartbeatExamples.VALID;
        nodeAvailabilityService.onNewHeartbeat(NodeExamples.LOCAL_INET_ADDR, validHb);
        boolean isActive = NodeExamples.LOCAL_HOST_HB_30.isActive();
        assertTrue(isActive);
    }
}
