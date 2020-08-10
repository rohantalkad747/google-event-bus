package com.trident.load_balancer;

import com.trident.load_balancer.HeartbeatMonitor.HeartbeatAck;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class HeartbeatMonitorTest {

    private final HeartbeatMonitor heartbeatMonitor = new HeartbeatMonitor(ClusterExamples.SMALL_CLUSTER);

    @BeforeEach
    public void reset() {
        NodeExamples.reset();
    }

    @Test
    void whenValidHeartbeatThenShouldReturnAck() throws ExecutionException, InterruptedException {
        Future<HeartbeatAck> future = heartbeatMonitor.onHeartbeat(NodeExamples.LOCAL_INET_ADDR, HeartbeatExamples.VALID);
        HeartbeatAck ack = future.get();
        assertThat(ack.allRecorded(), is(true));
    }

    @Test
    void whenInvalidHeartbeatThenShouldReturnNack() throws ExecutionException, InterruptedException {
        Future<HeartbeatAck> future = heartbeatMonitor.onHeartbeat(NodeExamples.LOCAL_INET_ADDR, HeartbeatExamples.WITH_INVALID_CPU_USAGE);
        HeartbeatAck heartbeatAck = future.get();
        assertThat(heartbeatAck.allRecorded(), is(false));
    }

    @Test
    void whenStoppedThenShouldNotAcceptHeartbeats() {
        heartbeatMonitor.stop();
        Assertions.assertThrows(RuntimeException.class, () -> heartbeatMonitor.onHeartbeat(NodeExamples.REMOTE_INET_ADDR, HeartbeatExamples.WITH_INVALID_CPU_USAGE));
    }

    static final class HeartbeatExamples {
        static final Heartbeat VALID = Heartbeat
                .builder()
                .connections(5)
                .cpuUsage(0.23)
                .ramUsage(0.55)
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
