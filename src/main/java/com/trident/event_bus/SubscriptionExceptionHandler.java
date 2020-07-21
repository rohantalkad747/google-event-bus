package com.trident.event_bus;

public interface SubscriptionExceptionHandler {
    /**
     * Handles exceptions thrown by subscribers.
     */
    void handleException(Throwable exception, SubscriberExceptionContext context);
}
