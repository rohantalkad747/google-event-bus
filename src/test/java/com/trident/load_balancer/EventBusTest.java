package com.trident.load_balancer;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.internal.util.collections.Sets;

import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;

@Slf4j
public class EventBusTest {
    private final Set<Object> sink = Sets.newSet();
    private final EventBus eventBus = new EventBus("test");

    // This listener puts all items in sink
    private final Object listener = new Object() {
        @Subscribe
        public void onNewEvent(Integer i) {
            log.info("Received event " + i);
            sink.add(i);
        }

        @Subscribe
        public void onNewEvent(String i) {
            log.info("Received event " + i);
            sink.add(i);
        }
    };

    @BeforeEach
    public void setUp() {
        eventBus.register(listener);
    }

    @Test
    public void testUnregister() {
        eventBus.unregister(listener);
        eventBus.post("string event");
        eventBus.post(9000);
        assertThat(sink, is(empty()));
    }

    @Test
    public void testPost() {
        eventBus.post("string event");
        eventBus.post(9000);
        assertThat(sink, is(not(empty())));
        assertThat(sink, hasSize(2));
    }
}