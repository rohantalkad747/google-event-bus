package com.trident.load_balancer;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import lombok.*;

import java.net.InetAddress;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@AllArgsConstructor
@ToString
public class Node {

    @Data
    @AllArgsConstructor
    static final class Stat<T extends Number> {
        private long epochMsStatWasGeneratedByNode;
        private T value;
    }

    private final Cache<Component, Number> componentUsage;
    @Getter
    @Setter
    private InetAddress ipAddress;
    @Getter(AccessLevel.NONE)
    @Setter
    private boolean isActive;

    private Node(long expectedHeartbeatPeriod, InetAddress ipAddress, boolean isActive) {
        this.isActive = isActive;
        this.ipAddress = ipAddress;
        this.componentUsage = CacheBuilder
                .newBuilder()
                .expireAfterAccess(expectedHeartbeatPeriod, TimeUnit.SECONDS)
                .build();
    }

    private <T> T getUsage(Component component, Function<? super Number, T> numberMapper) {
        return Optional.ofNullable(componentUsage.getIfPresent(component))
                .map(numberMapper)
                .orElse(null);
    }

    public void updateComponentUsage(Component component, Number usage) {
        componentUsage.put(component, usage);
    }

    public Integer getConnections() {
        return getUsage(Component.CONNECTIONS, Number::intValue);
    }

    public Double getRamUsage() {
        return getUsage(Component.RAM, Number::doubleValue);
    }

    public Double getCpuUsage() {
        return getUsage(Component.CPU, Number::doubleValue);
    }

    public boolean isActive() {
        return isActive;
    }

    public static class Builder {
        private boolean isActive = true;
        private InetAddress ipAddress;
        private long heartbeatPeriod;

        public Builder withIpAddress(InetAddress ipAddress) {
            this.ipAddress = ipAddress;
            return this;
        }

        public Builder withIsActive(boolean isActive) {
            this.isActive = isActive;
            return this;
        }

        public Builder withHeartbeatPeriod(long heartbeatPeriod) {
            this.heartbeatPeriod = heartbeatPeriod;
            return this;
        }

        public Node build() {
            return new Node(heartbeatPeriod, ipAddress, isActive);
        }
    }
}
