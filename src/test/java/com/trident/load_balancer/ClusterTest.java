package com.trident.load_balancer;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.collection.IsEmptyCollection.empty;


class ClusterTest {
    Cluster cluster = new Cluster();

    @Test
    void addNode() {
        cluster.addNode(NodeExamples.LOCAL_HOST_HB_30);
        assertThat(cluster.exists(NodeExamples.LOCAL_INET_ADDR), is(true));
    }

    @Test
    void removeNode() {
        cluster.addNode(NodeExamples.LOCAL_HOST_HB_30);
        cluster.removeNode(NodeExamples.LOCAL_HOST_HB_30);
        assertThat(cluster.exists(NodeExamples.LOCAL_INET_ADDR), is(false));

    }

    @Test
    void getAvailableNodes() {
        cluster.addNode(NodeExamples.LOCAL_HOST_HB_30);
        assertThat(cluster.getAvailableNodes(), hasSize(1));
        NodeExamples.LOCAL_HOST_HB_30.setActive(false);
        assertThat(cluster.getAvailableNodes(), empty());
    }

    @Test
    void getNode() {
        cluster.addNode(NodeExamples.LOCAL_HOST_HB_30);
        assertThat(
                cluster.getNode(NodeExamples.LOCAL_INET_ADDR),
                is(NodeExamples.LOCAL_HOST_HB_30)
        );
    }
}