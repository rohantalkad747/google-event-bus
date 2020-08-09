package com.trident.load_balancer;

import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Test;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class VMStatsClientTest {
    private final VirtualMachineUsageClient vmStatsClient = new VirtualMachineUsageClient();

    @Test
    void testRAMUsage() {
        double ramUsage = vmStatsClient.getCurrentRAMUsage();
        MatcherAssert.assertThat(ramUsage, greaterThanOrEqualTo(0d));
    }

    @Test
    void testCPUUsage() {
        double cpuUsage = vmStatsClient.getCurrentCPUUsage();
        MatcherAssert.assertThat(cpuUsage, greaterThanOrEqualTo(0d));
    }
}
