package com.trident.load_balancer;

import java.net.*;

public class NodeExamples {
    static InetAddress REMOTE_INET_ADDR;
    static InetAddress LOCAL_INET_ADDR;
    static Node LOCAL_HOST_HB_30;
    static Node REMOTE_HB_60;

    static void reset() {
        initNodes();
    }

    static {
        try {
            REMOTE_INET_ADDR = InetAddress.getByName("facebook.com");
            LOCAL_INET_ADDR = InetAddress.getByName("127.0.0.1");
            REMOTE_INET_ADDR = InetAddress.getByName("facebook.com");
            initNodes();
        } catch (UnknownHostException e) {
            System.exit(1);
        }
    }

    private static void initNodes() {
        LOCAL_HOST_HB_30 = new Node.Builder()
                .withHeartbeatPeriod(30)
                .withIpAddress(LOCAL_INET_ADDR)
                .build();
        REMOTE_HB_60 = new Node.Builder()
                .withHeartbeatPeriod(60)
                .withIpAddress(REMOTE_INET_ADDR)
                .build();
    }
}
