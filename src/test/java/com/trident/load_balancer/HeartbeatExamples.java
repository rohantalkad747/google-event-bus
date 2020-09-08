package com.trident.load_balancer;

import java.time.Instant;

final class HeartbeatExamples {

    static final Heartbeat VALID = Heartbeat
            .builder()
            .ipAddress(NodeExamples.LOCAL_HOST_8080)
            .connections(5)
            .cpuUsage(0.23)
            .ramUsage(0.55)
            .timeEpochMs(Instant.now().toEpochMilli())
            .build();
    static final Heartbeat ALL_INVALID = Heartbeat
            .builder()
            .ipAddress(NodeExamples.LOCAL_HOST_8080)
            .connections(-1)
            .cpuUsage(-1d)
            .ramUsage(-1d)
            .timeEpochMs(Instant.now().toEpochMilli())
            .build();
    static final Heartbeat WITH_INVALID_CPU_USAGE = Heartbeat
            .builder()
            .ipAddress(NodeExamples.LOCAL_HOST_8080)
            .connections(5)
            .cpuUsage(-1d)
            .ramUsage(0.55)
            .timeEpochMs(Instant.now().toEpochMilli())
            .build();

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
}
