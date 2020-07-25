package com.trident.load_balancer;

import lombok.Data;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.Executor;

@Data
public class Subscriber {
    private final Object target;
    private final Method method;
    private final Executor executor;
    private final EventBus eventBus;

    private Subscriber(EventBus eventBus, Object target, Method method) {
        this.eventBus = eventBus;
        this.target = target;
        this.method = method;
        this.method.setAccessible(true);
        this.executor = eventBus.getExecutor();
    }

    static Subscriber getInstance(EventBus eventBus, Object target, Method method) {
        if (methodIsThreadSafe(method)) {
            return new Subscriber(eventBus, target, method);
        }
        return new SynchronizedSubscriber(eventBus, target, method);
    }

    private static boolean methodIsThreadSafe(Method method) {
        return method.isAnnotationPresent(ConcurrentEventsAllowed.class);
    }

    final void dispatchEvent(@NonNull Object event) {
        executor.execute(() -> {
            try {
                invokeSubscriptionMethod(event);
            } catch (IllegalAccessException | InvocationTargetException e) {
                eventBus.handleInvocationException(e.getCause(), new SubscriberExceptionContext(event, target, method));
            }
        });
    }

    void invokeSubscriptionMethod(Object event) throws InvocationTargetException, IllegalAccessException {
        method.invoke(target, event);
    }

    static final class SynchronizedSubscriber extends Subscriber {

        public SynchronizedSubscriber(EventBus eventBus, Object target, Method method) {
            super(eventBus, target, method);
        }

        @Override
        void invokeSubscriptionMethod(Object event) throws InvocationTargetException, IllegalAccessException {
            synchronized (this) {
                super.invokeSubscriptionMethod(event);
            }
        }
    }


}
