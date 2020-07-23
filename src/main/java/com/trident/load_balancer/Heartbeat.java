package com.trident.load_balancer;

import lombok.Data;

@Data
public class Heartbeat {
    private double timeEpochMs;
    private String ipAddress;
    private int port;
    private double ramUsage;
    private double cpuUsage;
    private double connections;
}
