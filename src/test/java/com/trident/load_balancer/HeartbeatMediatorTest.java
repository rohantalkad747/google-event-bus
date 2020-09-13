package com.trident.load_balancer;

import com.google.common.collect.Maps;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class HeartbeatMediatorTest {
    private static final EventBus eBus = new EventBus("CLUSTER_HALF_SECOND_HB HB Mediator");
    private static HeartbeatMediator heartbeatMediator;

    private static final Long lh8080ExampleHbTs = HeartbeatExamples.VALID.getTimeEpochMs();


    @BeforeAll
    static void setup() {
        HashMap<Node, Long> hbLog = Maps.newHashMap();

        WebClient webClient = Mockito.mock(WebClient.class);

        NodeAvailabilityReceiver nodeAvailabilityReceiver = new NodeAvailabilityReceiver(ClusterExamples.CLUSTER_HALF_SECOND_HB, webClient, Duration.of(500, ChronoUnit.MILLIS), hbLog);

        NodeComponentUpdateReceiver nodeComponentUpdateReceiver = new NodeComponentUpdateReceiver(ClusterExamples.CLUSTER_HALF_SECOND_HB);

        eBus.register(nodeAvailabilityReceiver, nodeComponentUpdateReceiver);

        heartbeatMediator = new HeartbeatMediator(
            new EventBus("CLUSTER_HALF_SECOND_HB HB Mediator"),
            ClusterExamples.CLUSTER_HALF_SECOND_HB,
            Maps.newHashMap()
    );
    }

    @Test
    void testNewHbRecorded() throws InterruptedException {
        heartbeatMediator.onHeartbeat(HeartbeatExamples.VALID);
        expectDefaultTs();

        Heartbeat outOfSyncHb = HeartbeatExamples.randomHbWithTimestamp(lh8080ExampleHbTs - 1, NodeExamples.LOCAL_HOST_8080);
        heartbeatMediator.onHeartbeat(outOfSyncHb);
        expectDefaultTs();

        Heartbeat validHb = HeartbeatExamples.randomHbWithTimestamp(System.currentTimeMillis(), NodeExamples.NODE_8383.getHostName());
        heartbeatMediator.onHeartbeat(validHb);
        assertThat(ClusterExamples.CLUSTER_HALF_SECOND_HB.getNode(NodeExamples.LOCAL_HOST_8080).isActive(), is(true));

        Thread.sleep(1000);

        assertThat(ClusterExamples.CLUSTER_HALF_SECOND_HB.getNode(NodeExamples.LOCAL_HOST_8080).isActive(), is(false));
    }

    private void expectDefaultTs() {
        Long latestHeartbeatTimestampFromNode = heartbeatMediator.getLatestHeartbeatTimestampFromNode(
                NodeExamples.LOCAL_HOST_8080);
        assertThat(latestHeartbeatTimestampFromNode, is(lh8080ExampleHbTs));
    }

}