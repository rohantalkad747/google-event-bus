package com.trident.load_balancer;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.time.Instant;

@Data
@AllArgsConstructor
@Builder
@EqualsAndHashCode
public class Heartbeat {
    private Long timeEpochMs;
    private Double ramUsage;
    private Double cpuUsage;
    private Integer connections;
    private NodeComponentUsageService nodeComponentUsageService;

    public Heartbeat(NodeComponentUsageService nodeComponentUsageService) {
        this.nodeComponentUsageService = nodeComponentUsageService;
    }

    public Heartbeat nextHeartbeat() {
        return Heartbeat
                .builder()
                .timeEpochMs(Instant.now().toEpochMilli())
                .ramUsage(nodeComponentUsageService.getCurrentRAMUsage())
                .cpuUsage(nodeComponentUsageService.getCurrentCPUUsage())
                .nodeComponentUsageService(nodeComponentUsageService)
                .build();
    }
}
