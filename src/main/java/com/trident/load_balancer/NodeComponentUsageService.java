package com.trident.load_balancer;


import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.GlobalMemory;
import oshi.hardware.HardwareAbstractionLayer;

import static oshi.hardware.CentralProcessor.TickType.IDLE;
import static oshi.hardware.CentralProcessor.TickType.IOWAIT;

public class NodeComponentUsageService {
    private final SystemInfo si = new SystemInfo();

    private final HardwareAbstractionLayer hal = si.getHardware();

    private final CentralProcessor centralProcessor = hal.getProcessor();

    private long[] oldCPUTicks = new long[CentralProcessor.TickType.values().length];

    /**
     * @return the percentage of CPU currently being used.
     */
    public double getCurrentCPUUsage() {
        long[] newCPUTicks = centralProcessor.getSystemCpuLoadTicks();
        long totalCPUTicks = calculateTotalCPUTicks(newCPUTicks);
        long idleCPUTicks = getIdleTicks(newCPUTicks) - getIdleTicks(oldCPUTicks);
        oldCPUTicks = newCPUTicks;
        if (ticksValid(totalCPUTicks, idleCPUTicks)) {
            return (1 - (((double) idleCPUTicks) / totalCPUTicks));
        }
        return 0;
    }

    private boolean ticksValid(long totalCPUTicks, long idleTicks) {
        return totalCPUTicks > 0 && idleTicks > 0;
    }

    private long calculateTotalCPUTicks(long[] newCPUTicks) {
        long totalTicks = 0;
        for (int i = 0; i < oldCPUTicks.length; i++) {
            totalTicks += newCPUTicks[i] - oldCPUTicks[i];
        }
        return totalTicks;
    }

    private long getIdleTicks(long[] ticks) {
        return ticks[IDLE.getIndex()] + ticks[IOWAIT.getIndex()];
    }

    /**
     * @return the percentage of RAM currently being used.
     */
    public double getCurrentRAMUsage() {
        GlobalMemory memory = hal.getMemory();
        return 1 - ((double) memory.getAvailable()) / memory.getTotal();
    }
}
