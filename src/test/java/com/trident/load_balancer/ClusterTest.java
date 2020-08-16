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
        cluster.addNode(NodeExamples.NODE_8383);
        assertThat(cluster.exists(NodeExamples.LOCAL_HOST_8080), is(true));
    }

    @Test
    void removeNode() {
        cluster.addNode(NodeExamples.NODE_8383);
        cluster.removeNode(NodeExamples.NODE_8383);
        assertThat(cluster.exists(NodeExamples.LOCAL_HOST_8080), is(false));

    }

    @Test
    void getAvailableNodes() {
        cluster.addNode(NodeExamples.NODE_8383);
        assertThat(cluster.getAvailableNodes(), hasSize(1));
        NodeExamples.NODE_8383.setActive(false);
        assertThat(cluster.getAvailableNodes(), empty());
    }

    @Test
    void getNode() {
        cluster.addNode(NodeExamples.NODE_8383);
        assertThat(
                cluster.getNode(NodeExamples.LOCAL_HOST_8080).get(),
                is(NodeExamples.NODE_8383)
        );
    }
}