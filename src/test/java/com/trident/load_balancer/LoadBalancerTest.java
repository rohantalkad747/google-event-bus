//package com.talkad.load_balancer;
//
//import org.junit.Before;
//import org.junit.Test;
//
//import java.util.List;
//import java.util.concurrent.Callable;
//
//public class LoadBalancerTest {
//
//    Cluster cluster;
//
//    @Before
//    public void setUp() {
//        cluster = ExampleMachines.typicalCluster();
//    }
//
//    @Test
//    public void testDirectTraffic() {
//        LoadBalancer loadBalancer = new LoadBalancer(cluster);
//        Callable<Integer> randomTask = () -> {
//            for (int j = 0; j < 100; j++);
//            return 100;
//        };
//        for (int i = 0; i < 100; i++) {
//            loadBalancer.handleTask(randomTask);
//        }
//    }
//}
