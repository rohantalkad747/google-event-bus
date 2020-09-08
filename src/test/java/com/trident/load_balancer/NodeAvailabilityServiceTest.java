package com.trident.load_balancer;

import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.web.reactive.function.client.WebClient;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
class NodeAvailabilityServiceTest {
    private final Cluster clusterTarget = ClusterExamples.CLUSTER_HALF_SECOND_HB;
    private final WebClient webClient = WebClient.create();
    private final NodeAvailabilityReceiver nodeAvailabilityService = new NodeAvailabilityReceiver(clusterTarget, webClient, Duration.ofSeconds(2), Maps.newHashMap());
    public static MockWebServer mockBackEnd;

    @BeforeAll
    static void setUp() throws IOException {
        mockBackEnd = new MockWebServer();
        mockBackEnd.start();
        System.out.println(mockBackEnd.getHostName());
        System.out.println(mockBackEnd.getPort());
    }

    @AfterAll
    static void tearDown() throws IOException {
        mockBackEnd.shutdown();
    }


    @AfterEach
    void afterEach() {
        ClusterExamples.reset();
    }

    @Test
    void testHb() {
        assertTrue(NodeExamples.NODE_8383.isActive());
        Heartbeat validHb = HeartbeatExamples.VALID;
        nodeAvailabilityService.onNewHeartbeat(NodeExamples.LOCAL_HOST_8080, validHb);
        assertTrue(NodeExamples.NODE_8383.isActive());
    }

    @Test
    void testNoHbOnePeriod() throws InterruptedException {
        Heartbeat validHb = HeartbeatExamples.VALID;
        nodeAvailabilityService.onNewHeartbeat(NodeExamples.LOCAL_HOST_8080, validHb);
        TimeUnit.MILLISECONDS.sleep(clusterTarget.getHeartbeatPeriodMs());
        boolean isActive = NodeExamples.NODE_8383.isActive();
        assertTrue(isActive);
    }

    @Test
    void testNoHbTwoPeriods() throws InterruptedException {
        mockBackEnd.enqueue(new MockResponse().setResponseCode(400));

        // Test for timeout and one 400 response
        TimeUnit.MILLISECONDS.sleep(5000 + (3 * clusterTarget.getHeartbeatPeriodMs()));
        log.info("Checking whether nodes are active!");
        assertFalse(NodeExamples.NODE_8383.isActive());
        assertFalse(NodeExamples.NODE_8080.isActive());
    }

    @Test
    void testReavailabilityOnNewHb() throws InterruptedException {
        TimeUnit.MILLISECONDS.sleep(3 * clusterTarget.getHeartbeatPeriodMs());
        Heartbeat validHb = HeartbeatExamples.VALID;
        nodeAvailabilityService.onNewHeartbeat(NodeExamples.LOCAL_HOST_8080, validHb);
        boolean isActive = NodeExamples.NODE_8383.isActive();
        assertTrue(isActive);
    }

    @Test
    void testReavailabilityOnResponsivePing() throws InterruptedException {
        Node MY_ACTIVE_NODE = new Node.Builder()
                .withHeartbeatPeriod(30)
                .withIpAddress("localhost:" + mockBackEnd.getPort())
                .build();
        clusterTarget.addNode(MY_ACTIVE_NODE);

        mockBackEnd.enqueue(new MockResponse().setResponseCode(200));

        TimeUnit.MILLISECONDS.sleep(500 + (2 * clusterTarget.getHeartbeatPeriodMs()));
        boolean isActive = MY_ACTIVE_NODE.isActive();
        assertTrue(isActive);
    }
}
