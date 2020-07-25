package com.trident.load_balancer;

public interface SubscriptionExceptionHandler {
    /**
     * Handles exceptions thrown by subscribers.
     */
    void handleException(Throwable exception, SubscriberExceptionContext context);
}
