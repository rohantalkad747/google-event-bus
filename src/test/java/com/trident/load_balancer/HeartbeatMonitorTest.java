package com.trident.load_balancer;

import com.trident.load_balancer.HeartbeatMonitor.HeartbeatAck;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class HeartbeatMonitorTest {
    static final class HeartbeatExamples {
        static final Heartbeat VALID_HEARTBEAT = Heartbeat
                .builder()
                .connections(5)
                .cpuUsage(0.23)
                .ramUsage(0.55)
                .timeEpochMs(Instant.now().toEpochMilli())
                .build();

        static final Heartbeat INVALID_HEARTBEAT = Heartbeat
                .builder()
                .connections(5)
                .cpuUsage(-1)
                .ramUsage(0.55)
                .timeEpochMs(Instant.now().toEpochMilli())
                .build();
    }

    private final HeartbeatMonitor heartbeatMonitor = new HeartbeatMonitor(ClusterExamples.SMALL_CLUSTER);

    @Test
    void whenValidHeartbeatThenShouldReturnAck() throws ExecutionException, InterruptedException {
        Future<HeartbeatAck> future = heartbeatMonitor.onHeartbeat(HeartbeatExamples.VALID_HEARTBEAT);
        HeartbeatAck ack = future.get();
        assertThat(ack.allRecorded(), is(true));
    }

    @Test
    void whenInvalidHeartbeatThenShouldReturnNack() throws ExecutionException, InterruptedException {
        Future<HeartbeatAck> future = heartbeatMonitor.onHeartbeat(HeartbeatExamples.INVALID_HEARTBEAT);
        HeartbeatAck heartbeatAck = future.get();
        assertThat(heartbeatAck.allRecorded(), is(false));
        assertThat(heartbeatAck.getAckedComponents(), not(contains(Component.CPU)));
    }

    @Test
    void whenStoppedThenShouldNotAcceptHeartbeats() {
        heartbeatMonitor.stop();
        Assertions.assertThrows(RuntimeException.class, () -> heartbeatMonitor.onHeartbeat(HeartbeatExamples.INVALID_HEARTBEAT));
    }
}
