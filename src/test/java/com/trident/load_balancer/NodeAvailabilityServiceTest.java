package com.trident.load_balancer;

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class NodeAvailabilityServiceTest {
    private final NodeAvailabilityService nodeAvailabilityService = new NodeAvailabilityService(ClusterExamples.SMALL_CLUSTER);

    @Test
    void testHb() {
        Heartbeat validHb = HeartbeatMonitorTest.HeartbeatExamples.VALID;
        assertThat(NodeExamples.LOCAL_HOST_HB_30.isActive(), is(true));
        nodeAvailabilityService.onNewHeartbeat(validHb);
        assertThat(NodeExamples.LOCAL_HOST_HB_30.isActive(), is(true));
    }

    @Test
    void testNoHbOnePeriod() throws InterruptedException {
        Heartbeat validHb = HeartbeatMonitorTest.HeartbeatExamples.VALID;
        nodeAvailabilityService.onNewHeartbeat(validHb);
        // Given at least a 30 second break from the last heartbeat
        TimeUnit.MILLISECONDS.sleep(30);
        // When we ask if this node is active
        boolean isActive = NodeExamples.LOCAL_HOST_HB_30.isActive();
        // Then it should still be true since it's hasn't been 60 seconds
        assertThat(isActive, is(true));
    }

    @Test
    void testNoHbTwoPeriods() throws InterruptedException {
        Heartbeat validHb = HeartbeatMonitorTest.HeartbeatExamples.VALID;
        // Given at least a 60 second break from initialization
        TimeUnit.MILLISECONDS.sleep(60);
        // When we ask if this node is active
        boolean isActive = NodeExamples.LOCAL_HOST_HB_30.isActive();
        // Then it should still be false since at least 2 periods passed with no heartbeat for this node
        assertThat(isActive, is(false));
    }

    @Test
    void testMultipleInvalidNodes() throws InterruptedException {
        Heartbeat invalidHb = HeartbeatMonitorTest.HeartbeatExamples.WITH_INVALID_CPU_USAGE;
        nodeAvailabilityService.onNewHeartbeat(invalidHb);
        TimeUnit.MILLISECONDS.sleep(30);
        nodeAvailabilityService.onNewHeartbeat(invalidHb);
        TimeUnit.MILLISECONDS.sleep(60);
        boolean isActive = NodeExamples.LOCAL_HOST_HB_30.isActive();
        assertThat(isActive, is(false));
    }

    @Test
    void testReavailability() throws InterruptedException {
        TimeUnit.MILLISECONDS.sleep(60);
        Heartbeat validHb = HeartbeatMonitorTest.HeartbeatExamples.VALID;
        nodeAvailabilityService.onNewHeartbeat(validHb);
        boolean isActive = NodeExamples.LOCAL_HOST_HB_30.isActive();
        assertThat(isActive, is(false));
    }
}
