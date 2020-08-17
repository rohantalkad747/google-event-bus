//package com.trident.load_balancer;
//
//import org.junit.jupiter.api.Test;
//
//import static org.hamcrest.MatcherAssert.assertThat;
//import static org.hamcrest.Matchers.*;
//import static org.junit.jupiter.api.Assertions.assertThrows;
//
//class HeartbeatMediatorTest {
//    private final HeartbeatMediator heartbeatMediator = new HeartbeatMediator(
//            new EventBus("CLUSTER_HALF_SECOND_HB HB Mediator"),
//            ClusterExamples.CLUSTER_HALF_SECOND_HB
//    );
//    private final Long lh8080ExampleHbTs = HeartbeatMonitorTest.HeartbeatExamples.VALID.getTimeEpochMs();
//
//
//    @Test
//    void testNewHbRecorded() {
//        heartbeatMediator.onHeartbeat(HeartbeatMonitorTest.HeartbeatExamples.VALID);
//        expectDefaultTs();
//    }
//
//    private void expectDefaultTs() {
//        Long latestHeartbeatTimestampFromNode = heartbeatMediator.getLatestHeartbeatTimestampFromNode(
//                NodeExamples.LOCAL_HOST_8080);
//        assertThat(latestHeartbeatTimestampFromNode, is(lh8080ExampleHbTs));
//    }
//
//    @Test
//    void testOutOfSynchHbNotOverwritten() {
//        Heartbeat outOfSyncHb = HeartbeatMonitorTest.HeartbeatExamples.randomHbWithTimestamp(lh8080ExampleHbTs - 1);
//        assertThrows(OutOfSyncHeartbeatException.class, () -> {
//            heartbeatMediator.onHeartbeat(outOfSyncHb);
//            expectDefaultTs();
//        });
//    }
//}