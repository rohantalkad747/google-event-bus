package com.trident.load_balancer;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@AllArgsConstructor
@ToString
public class Node {
    private final Cache<Component, Number> componentUsage;
    @Getter
    private URI uri;
    @Getter(AccessLevel.NONE)
    private final boolean isActive;

    private Node(long expectedHeartbeatPeriod, URI uri) {
        this.isActive = true;
        this.uri = uri;
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

    public void updateURI(URI uri) {
        this.uri = uri;
    }

    public static class Builder {
        private URI uri;
        private long heartbeatPeriod;

        public Builder withURI(URI uri) {
            this.uri = uri;
            return this;
        }

        public Builder withHeartbeatPeriod(long heartbeatPeriod) {
            this.heartbeatPeriod = heartbeatPeriod;
            return this;
        }

        public Node build() {
            return new Node(heartbeatPeriod, uri);
        }
    }
}
