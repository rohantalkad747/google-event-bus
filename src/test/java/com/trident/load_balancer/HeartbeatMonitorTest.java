package com.trident.load_balancer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class HeartbeatMonitorTest {

    private final HeartbeatMonitor heartbeatMonitor = new HeartbeatMonitor(ClusterExamples.CLUSTER_HALF_SECOND_HB);

    @BeforeEach
    public void reset() {
        NodeExamples.reset();
    }

    @AfterAll
    public static void resetAll() {
        NodeExamples.reset();
    }

    @Test
    void whenValidHeartbeatThenShouldReturnAck() throws ExecutionException, InterruptedException {
        Future<HeartbeatAck> future = heartbeatMonitor.onHeartbeat(NodeExamples.LOCAL_HOST_8080, HeartbeatExamples.VALID);
        HeartbeatAck ack = future.get();
        assertThat(ack.allRecorded(), is(true));
    }

    @Test
    void whenInvalidHeartbeatThenShouldReturnNack() throws ExecutionException, InterruptedException {
        Future<HeartbeatAck> future = heartbeatMonitor.onHeartbeat(NodeExamples.LOCAL_HOST_8080, HeartbeatExamples.WITH_INVALID_CPU_USAGE);
        HeartbeatAck heartbeatAck = future.get();
        assertThat(heartbeatAck.allRecorded(), is(false));
    }

    @Test
    void whenStoppedThenShouldNotAcceptHeartbeats() {
        heartbeatMonitor.stop();
        Assertions.assertThrows(RuntimeException.class, () -> heartbeatMonitor.onHeartbeat(NodeExamples.LOCAL_HOST_8383, HeartbeatExamples.WITH_INVALID_CPU_USAGE));
    }

    static final class HeartbeatExamples {

        static Heartbeat randomHbWithTimestamp(long ts, String ip) {
            return Heartbeat
                    .builder()
                    .ipAddress(ip)
                    .connections(5)
                    .cpuUsage(0.23)
                    .ramUsage(0.55)
                    .timeEpochMs(ts)
                    .build();
        }

        static final Heartbeat VALID = Heartbeat
                .builder()
                .connections(5)
                .cpuUsage(0.23)
                .ramUsage(0.55)
                .timeEpochMs(Instant.now().toEpochMilli())
                .build();

        static final Heartbeat ALL_INVALID = Heartbeat
                .builder()
                .connections(-1)
                .cpuUsage(-1d)
                .ramUsage(-1d)
                .timeEpochMs(Instant.now().toEpochMilli())
                .build();

        static final Heartbeat WITH_INVALID_CPU_USAGE = Heartbeat
                .builder()
                .connections(5)
                .cpuUsage(-1d)
                .ramUsage(0.55)
                .timeEpochMs(Instant.now().toEpochMilli())
                .build();
    }
}
