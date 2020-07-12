package com.talkad;

import com.google.common.collect.Queues;
import lombok.AllArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Iterator;
import java.util.Queue;

abstract class Dispatcher {

    static Dispatcher getInstance(Type dispatcherType) {
        Dispatcher dispatcher;
        switch (dispatcherType) {
            case IMMEDIATE:
                dispatcher = ImmediateDispatcher.INSTANCE;
                break;
            case PER_THREAD:
                dispatcher = new PerThreadQueueDispatcher();
                break;
            case ASYNC:
                dispatcher = new AsyncDispatcher();
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + dispatcherType);
        }
        return dispatcher;
    }

    abstract void dispatch(Object event, Iterator<Subscriber> subscriberIterator);

    enum Type {IMMEDIATE, PER_THREAD, ASYNC}

    @AllArgsConstructor
    private static final class Event {
        private final Object event;
        private final Iterator<Subscriber> subscriberIterator;
    }

    @AllArgsConstructor
    private static final class EventSubscriberPair {
        private final Object event;
        private final Subscriber subscriber;
    }

    private static final class AsyncDispatcher extends Dispatcher {
        private final Queue<EventSubscriberPair> queue = Queues.newConcurrentLinkedQueue();

        @Override
        void dispatch(@NonNull Object event, @NonNull Iterator<Subscriber> subscriberIterator) {
            subscriberIterator.forEachRemaining(sub -> queue.add(new EventSubscriberPair(event, sub)));
            queue.forEach(eventSubscriberPair -> eventSubscriberPair.subscriber.dispatchEvent(eventSubscriberPair.event));
        }
    }

    private static final class ImmediateDispatcher extends Dispatcher {
        private static final ImmediateDispatcher INSTANCE = new ImmediateDispatcher();

        @Override
        void dispatch(@NonNull Object event, @NonNull Iterator<Subscriber> subscriberIterator) {
            while (subscriberIterator.hasNext()) {
                subscriberIterator.next().dispatchEvent(event);
            }
        }
    }

    private static final class PerThreadQueueDispatcher extends Dispatcher {

        private final ThreadLocal<Queue<Event>> eventQueue = ThreadLocal.withInitial(Queues::newArrayDeque);
        private final ThreadLocal<Boolean> dispatching = ThreadLocal.withInitial(() -> Boolean.FALSE);

        /**
         * Allows events to be added to the event queue reentrantly but dispatches them in the order they were received.
         */
        @Override
        void dispatch(@NonNull Object event, @NonNull Iterator<Subscriber> subscriberIterator) {
            Queue<Event> events = eventQueue.get();
            events.add(new Event(event, subscriberIterator));
            if (!dispatching.get()) {
                dispatching.set(true);
                try {
                    Event nextEvent;
                    while ((nextEvent = events.poll()) != null) {
                        while (nextEvent.subscriberIterator.hasNext()) {
                            subscriberIterator.next().dispatchEvent(nextEvent.event);
                        }
                    }
                } finally {
                    dispatching.remove();
                    eventQueue.remove();
                }
            }
        }

    }
}
