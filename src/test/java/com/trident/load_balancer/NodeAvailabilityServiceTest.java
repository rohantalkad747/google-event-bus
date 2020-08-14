package com.trident.load_balancer;

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class NodeAvailabilityServiceTest {
    private final Cluster clusterTarget = ClusterExamples.CLUSTER_HALF_SECOND_HB;
    private final NodeAvailabilityService nodeAvailabilityService = new NodeAvailabilityService(clusterTarget);

    @Test
    void testHb() {
        assertTrue(NodeExamples.LOCAL_HOST.isActive());
        Heartbeat validHb = HeartbeatMonitorTest.HeartbeatExamples.VALID;
        nodeAvailabilityService.onNewHeartbeat(NodeExamples.LOCAL_INET_ADDR, validHb);
        assertTrue(NodeExamples.LOCAL_HOST.isActive());
    }

    @Test
    void testNoHbOnePeriod() throws InterruptedException {
        Heartbeat validHb = HeartbeatMonitorTest.HeartbeatExamples.VALID;
        nodeAvailabilityService.onNewHeartbeat(NodeExamples.LOCAL_INET_ADDR, validHb);
        // Given at least a 500 ms break from the last heartbeat
        TimeUnit.MILLISECONDS.sleep(clusterTarget.getHeartbeatPeriodMs());
        // When we ask if this node is active
        boolean isActive = NodeExamples.LOCAL_HOST.isActive();
        // Then it should still be true since it's been one second
        assertTrue(isActive);
    }

    @Test
    void testNoHbTwoPeriods() throws InterruptedException {
        // Given at least a 60 second break from initialization
        TimeUnit.MILLISECONDS.sleep(2 * clusterTarget.getHeartbeatPeriodMs());
        // When we ask if this node is active
        boolean isActive = NodeExamples.LOCAL_HOST.isActive();
        // Then it should still be false since at least 2 periods passed with no heartbeat for this node
        assertFalse(isActive);
    }

    @Test
    void testMultipleInvalidNodes() throws InterruptedException {
        Heartbeat invalidHb = HeartbeatMonitorTest.HeartbeatExamples.WITH_INVALID_CPU_USAGE;
        nodeAvailabilityService.onNewHeartbeat(NodeExamples.LOCAL_INET_ADDR, invalidHb);
        TimeUnit.MILLISECONDS.sleep(clusterTarget.getHeartbeatPeriodMs());
        nodeAvailabilityService.onNewHeartbeat(NodeExamples.LOCAL_INET_ADDR, invalidHb);
        TimeUnit.MILLISECONDS.sleep(2 * clusterTarget.getHeartbeatPeriodMs());
        boolean isActive = NodeExamples.LOCAL_HOST.isActive();
        assertFalse(isActive);
    }

    @Test
    void testReavailability() throws InterruptedException {
        TimeUnit.MILLISECONDS.sleep(2 * clusterTarget.getHeartbeatPeriodMs());
        Heartbeat validHb = HeartbeatMonitorTest.HeartbeatExamples.VALID;
        nodeAvailabilityService.onNewHeartbeat(NodeExamples.LOCAL_INET_ADDR, validHb);
        boolean isActive = NodeExamples.LOCAL_HOST.isActive();
        assertTrue(isActive);
    }
}
