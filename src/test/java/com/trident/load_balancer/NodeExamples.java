package com.trident.load_balancer;

public class NodeExamples {
    static String REMOTE_INET_ADDR = "176.13.69.63";
    static String LOCAL_INET_ADDR = "127.0.0.1";
    static Node LOCAL_HOST;
    static Node REMOTE;

    static void reset() {
        initNodes();
    }

    static {
        initNodes();
    }

    private static void initNodes() {
        LOCAL_HOST = new Node.Builder()
                .withHeartbeatPeriod(30)
                .withIpAddress(LOCAL_INET_ADDR)
                .build();
        REMOTE = new Node.Builder()
                .withHeartbeatPeriod(60)
                .withIpAddress(REMOTE_INET_ADDR)
                .build();
    }
}
