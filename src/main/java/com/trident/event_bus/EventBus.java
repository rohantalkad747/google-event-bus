package com.trident.event_bus;

import com.google.common.util.concurrent.MoreExecutors;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

@Slf4j
@Data
@AllArgsConstructor
public class EventBus {
    private final String name;
    private final Executor executor;
    private final SubscriptionExceptionHandler subscriptionExceptionHandler;
    private final Dispatcher dispatcher;
    private final SubscriptionRegistry subscriptionRegistry = new SubscriptionRegistry(this);

    public EventBus(String identifier) {
        this(identifier, MoreExecutors.directExecutor(), LoggingHandler.INSTANCE, Dispatcher.getInstance(Dispatcher.Type.PER_THREAD));
    }

    public EventBus(SubscriptionExceptionHandler exceptionHandler) {
        this("default", MoreExecutors.directExecutor(), exceptionHandler, Dispatcher.getInstance(Dispatcher.Type.PER_THREAD));
    }

    void handleInvocationException(@NonNull Throwable cause, @NonNull SubscriberExceptionContext subscriberExceptionContext) {
        try {
            subscriptionExceptionHandler.handleException(cause, subscriberExceptionContext);
        } catch (Throwable e) {
            log.error("Exception thrown by exception handler", e);
        }
    }

    public void register(Object listener) {
        try {
            subscriptionRegistry.register(listener);
        } catch (ExecutionException e) {
            throw new Error("Could not register listener!", e);
        }
    }

    public void unregister(Object listener) {
        try {
            subscriptionRegistry.unregister(listener);
        } catch (ExecutionException e) {
            throw new Error("Could not unregister listener!", e);
        }
    }

    public void post(Object event) {
        Iterator<Subscriber> subscribers = subscriptionRegistry.getSubscribers(event);
        if (subscribers.hasNext()) {
            dispatcher.dispatch(event, subscribers);
        } else if (!isDeadEvent(event)) {
            post(new DeadEvent(this, event));
        }
    }

    private boolean isDeadEvent(Object event) {
        return event instanceof DeadEvent;
    }

    @Slf4j
    static final class LoggingHandler implements SubscriptionExceptionHandler {
        static final LoggingHandler INSTANCE = new LoggingHandler();

        @Override
        public void handleException(Throwable exception, SubscriberExceptionContext context) {
            LoggingHandler.log.error(formatContext(context), exception);
        }

        private String formatContext(SubscriberExceptionContext context) {
            Method method = context.getMethod();
            return "Exception thrown by subscriber method "
                    + method.getName()
                    + '('
                    + method.getParameterTypes()[0].getName()
                    + ')'
                    + " on subscriber "
                    + context.getTarget()
                    + " when dispatching event: "
                    + context.getEvent();
        }
    }
}
