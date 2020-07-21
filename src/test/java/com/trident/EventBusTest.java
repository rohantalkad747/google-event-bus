package com.trident;

import com.trident.event_bus.EventBus;
import com.trident.event_bus.Subscribe;
import junit.framework.TestCase;
import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Test;

@Slf4j
public class EventBusTest extends TestCase {

    private EventBus eventBus;
    private Object listener;

    @Before
    public void setUp() {
        eventBus = new EventBus("test");
        listener = new Object() {
            @Subscribe
            public void onNewEvent(Integer i) {
                log.info("Received event " + i );
            }

            @Subscribe
            public void onNewEvent(String i) {
                log.info("Received event " + i );
            }
        };
    }



    @Test
    public void testRegister() {
        eventBus.register(listener);
    }

    @Test
    public void testUnregister() {
        eventBus.register(listener);
        eventBus.unregister(listener);
        eventBus.post("string event");
        eventBus.post(9000);
    }

    @Test
    public void testPost() {
        eventBus.register(listener);
        eventBus.post("string event");
        eventBus.post(9000);
    }
}