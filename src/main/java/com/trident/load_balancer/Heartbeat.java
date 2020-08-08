package com.trident.load_balancer;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;


import java.time.Instant;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Heartbeat {
    private double timeEpochMs;
    private double ramUsage;
    private double cpuUsage;
    private int connections;
    private VirtualMachineUsageClient vmStatsClient;

    public Heartbeat nextHeartbeat() {
        return Heartbeat
                .builder()
                .timeEpochMs(Instant.now().toEpochMilli())
                .ramUsage(vmStatsClient.getCurrentRAMUsage())
                .cpuUsage(vmStatsClient.getCurrentCPUUsage())
                .build();
    }
}
