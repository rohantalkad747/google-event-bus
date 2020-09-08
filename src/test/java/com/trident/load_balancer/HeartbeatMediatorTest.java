package com.trident.load_balancer;

import com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

class HeartbeatMediatorTest {
    private final HeartbeatMediator heartbeatMediator = new HeartbeatMediator(
            new EventBus("CLUSTER_HALF_SECOND_HB HB Mediator"),
            ClusterExamples.CLUSTER_HALF_SECOND_HB,
            Maps.newHashMap()
    );
    private final Long lh8080ExampleHbTs = HeartbeatExamples.VALID.getTimeEpochMs();


    @Test
    void testNewHbRecorded() {
        heartbeatMediator.onHeartbeat(HeartbeatExamples.VALID);
        expectDefaultTs();
        Heartbeat outOfSyncHb = HeartbeatExamples.randomHbWithTimestamp(lh8080ExampleHbTs - 1, NodeExamples.LOCAL_HOST_8080);
        heartbeatMediator.onHeartbeat(outOfSyncHb);
        expectDefaultTs();
    }

    private void expectDefaultTs() {
        Long latestHeartbeatTimestampFromNode = heartbeatMediator.getLatestHeartbeatTimestampFromNode(
                NodeExamples.LOCAL_HOST_8080);
        assertThat(latestHeartbeatTimestampFromNode, is(lh8080ExampleHbTs));
    }

}