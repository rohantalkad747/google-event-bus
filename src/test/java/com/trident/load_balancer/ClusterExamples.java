package com.trident.load_balancer;

import java.net.URI;
import java.net.URISyntaxException;

public class ClusterExamples {
    static URI LOCAL_HOST_8080;
    static Cluster SMALL_CLUSTER = null;

    static {
        try {
            LOCAL_HOST_8080 = new URI("http://localhost:8080");
            Node node = new Node.Builder()
                    .withHeartbeatPeriod(30)
                    .withURI(LOCAL_HOST_8080)
                    .build();
            SMALL_CLUSTER = new Cluster();
            SMALL_CLUSTER.addNode(node);
        } catch (URISyntaxException e) {
            System.exit(1);
        }
    }
}
