package com.trident.load_balancer;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.sun.management.OperatingSystemMXBean;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.concurrent.ExecutionException;

@AllArgsConstructor
public class VirtualMachine implements Node, Connectable {

    @Getter
    private final OperatingSystemMXBean osMXBean;
    @Getter
    private final FileStore fileStore;
    private final LoadingCache<Component, Double> usageStats;
    @Setter
    private java.net.URL URL;
    @Getter
    @Setter
    private boolean isActive;
    private Duration componentStatsExpiryTime = Duration.ofSeconds(1);
    @Getter
    private volatile int connections;

    @Override
    public synchronized void onConnect() {
        ++connections;
    }

    @Override
    public synchronized void onDisconnect() {
        --connections;
    }

    @Override
    public String getHostName() {
        return URL.getHost();
    }

    @Override
    public double getPercentUsage(Component component) {
        try {
            return usageStats.get(component);
        } catch (ExecutionException e) {
            return -1;
        }
    }


    VirtualMachine(java.net.URL address) throws IOException {
        this.URL = address;
        this.osMXBean = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
        this.fileStore = Files.getFileStore(Paths.get(System.getProperty("user.dir")));
        this.usageStats = initComponentUsageCache();
    }

    VirtualMachine(java.net.URL address, Duration componentStatsExpiryTime) throws IOException {
        this(address);
        this.componentStatsExpiryTime = componentStatsExpiryTime;
    }

    private LoadingCache<Component, Double> initComponentUsageCache() {
        return CacheBuilder
                .newBuilder()
                .expireAfterWrite(componentStatsExpiryTime)
                .build(new CacheLoader<>() {
                    @Override
                    public Double load(Component component) {
                        return component.getPercentUsage();
                    }
                });
    }

    enum Component {
        CPU {
            @Override
            double getPercentUsage() {
                return machine.osMXBean.getSystemLoadAverage() / machine.osMXBean.getAvailableProcessors();
            }
        },
        RAM {
            @Override
            double getPercentUsage() {
                return 1 - ((double) machine.osMXBean.getFreeMemorySize() / machine.osMXBean.getTotalMemorySize());
            }
        },
        DISK {
            @SneakyThrows
            @Override
            double getPercentUsage() {
                return 1 - ((double) machine.fileStore.getUsableSpace() / machine.fileStore.getTotalSpace());
            }
        };
        VirtualMachine machine;

        abstract double getPercentUsage();
    }
}
