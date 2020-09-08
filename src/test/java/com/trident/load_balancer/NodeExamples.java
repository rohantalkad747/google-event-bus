package com.trident.load_balancer;

public class NodeExamples {
    static String LOCAL_HOST_8383 = "localhost:8383";
    static String LOCAL_HOST_8080 = "localhost:8080";
    static Node NODE_8383;
    static Node NODE_8080;

    static {
        initNodes();
    }

    static void reset() {
        initNodes();
    }

    private static void initNodes() {
        NODE_8383 = new Node.Builder()
                .withHeartbeatPeriod(30)
                .withIpAddress(LOCAL_HOST_8080)
                .build();
        NODE_8080 = new Node.Builder()
                .withHeartbeatPeriod(60)
                .withIpAddress(LOCAL_HOST_8383)
                .build();
    }
}
