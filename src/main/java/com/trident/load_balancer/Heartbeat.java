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
    private String ipAddress;
    private Long timeEpochMs;
    private Double ramUsage;
    private Double cpuUsage;
    private Integer connections;
    private ComponentUsageService componentUsageService;

    public Heartbeat(ComponentUsageService componentUsageService) {
        this.componentUsageService = componentUsageService;
    }

    public Heartbeat nextHeartbeat() {
        return Heartbeat
                .builder()
                .ipAddress(ipAddress)
                .timeEpochMs(Instant.now().toEpochMilli())
                .ramUsage(componentUsageService.getCurrentRAMUsage())
                .cpuUsage(componentUsageService.getCurrentCPUUsage())
                .componentUsageService(componentUsageService)
                .build();
    }
}
