package com.trident.load_balancer;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import lombok.*;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

@AllArgsConstructor
@ToString
public class Node
{

    private final Cache<Component, Number> componentUsage;
    @Getter
    @Setter
    private String hostName;
    @Getter(AccessLevel.NONE)
    @Setter
    private AtomicBoolean isActive;

    private Node(long expectedHeartbeatPeriod, String ipAddress, boolean isActive)
    {
        this.isActive = new AtomicBoolean(isActive);
        this.hostName = ipAddress;
        this.componentUsage = CacheBuilder
                .newBuilder()
                .expireAfterAccess(2 * expectedHeartbeatPeriod, TimeUnit.SECONDS)
                .build();
    }

    private <T> T getUsage(Component component, Function<? super Number, T> numberMapper)
    {
        return Optional.ofNullable(componentUsage.getIfPresent(component))
                .map(numberMapper)
                .orElse(null);
    }

    public void updateComponentUsage(Component component, Number usage)
    {
        componentUsage.put(component, usage);
    }

    public Integer getConnections()
    {
        return getUsage(Component.CONNECTIONS, Number::intValue);
    }

    public Double getRamUsage()
    {
        return getUsage(Component.RAM, Number::doubleValue);
    }

    public Double getCpuUsage()
    {
        return getUsage(Component.CPU, Number::doubleValue);
    }

    public boolean isActive()
    {
        return isActive.get();
    }

    public void setActive(boolean active)
    {
        isActive.set(active);
    }

    @Data
    @AllArgsConstructor
    static final class Stat<T extends Number>
    {
        private long epochMsStatWasGeneratedByNode;
        private T value;
    }

    public static class Builder
    {
        private boolean isActive = true;
        private String ipAddress;
        private long heartbeatPeriod;

        public Builder withIpAddress(String ipAddress)
        {
            this.ipAddress = ipAddress;
            return this;
        }

        public Builder withIsActive(boolean isActive)
        {
            this.isActive = isActive;
            return this;
        }

        public Builder withHeartbeatPeriod(long heartbeatPeriod)
        {
            this.heartbeatPeriod = heartbeatPeriod;
            return this;
        }

        public Node build()
        {
            return new Node(heartbeatPeriod, ipAddress, isActive);
        }
    }
}
