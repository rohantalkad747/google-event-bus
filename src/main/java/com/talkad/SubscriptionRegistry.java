package com.talkad;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class SubscriptionRegistry {

    private final Map<Class<?>, CopyOnWriteArraySet<Subscriber>> subscribersByEventType = Maps.newConcurrentMap();
    private final EventBus eventBus;
    private final LoadingCache<Class<?>, ImmutableList<Class<?>>> superTypeCache = CacheBuilder.newBuilder()
            .weakKeys()
            .build(new CacheLoader<>() {
                @Override
                public ImmutableList<Class<?>> load(Class<?> clazz) {
                    return getSupertypes(clazz);
                }
            });

    SubscriptionRegistry(EventBus eventBus) {
        this.eventBus = eventBus;
    }

    /**
     * Gets all subscribers for the hierarchy of this event.
     */
    Iterator<Subscriber> getSubscribers(Object event) {
        ImmutableList<Class<?>> superTypes = superTypeCache.getUnchecked(event.getClass());
        return superTypes
                .stream()
                .map(subscribersByEventType::get)
                .filter(Objects::nonNull)
                .flatMap(Set::stream)
                .collect(toList())
                .iterator();
    }

    void register(Object listener) throws ExecutionException {
        Multimap<Class<?>, Subscriber> subscribersByEventTypeForListener = getSubscribersByEventType(listener);
        for (Map.Entry<Class<?>, Collection<Subscriber>> entry : subscribersByEventTypeForListener.asMap().entrySet()) {
            CopyOnWriteArraySet<Subscriber> eventSubscribers = subscribersByEventType.computeIfAbsent(entry.getKey(), k -> new CopyOnWriteArraySet<>());
            eventSubscribers.addAll(entry.getValue());
        }
    }

    void unregister(Object listener) throws ExecutionException {
        Multimap<Class<?>, Subscriber> subscribersByEventTypeForListener = getSubscribersByEventType(listener);
        subscribersByEventTypeForListener
                .asMap()
                .forEach((eventType, allSubscribersForThisEvent) -> {
                    CopyOnWriteArraySet<Subscriber> subscribersForThisEvent = subscribersByEventType.get(eventType);
                    if (subscribersForThisEvent != null) {
                        subscribersForThisEvent.removeAll(allSubscribersForThisEvent);
                    }
                });
    }

    private Multimap<Class<?>, Subscriber> getSubscribersByEventType(Object listener) throws ExecutionException {
        Multimap<Class<?>, Subscriber> subscriberMultimap = HashMultimap.create();
        for (Method method : getSubscribedMethods(listener.getClass())) {
            Class<?> eventType = getEventType(method);
            subscriberMultimap.put(eventType, Subscriber.getInstance(eventBus, listener, method));
        }
        return subscriberMultimap;
    }

    private Class<?> getEventType(Method method) {
        Class<?>[] parameterTypes = method.getParameterTypes();
        return parameterTypes[0];
    }

    /**
     * @return all the methods in the hierarchy of {@code clazz} with the annotation {@code annotation}.
     */
    private ImmutableList<Method> getSubscribedMethods(Class<?> clazz) throws ExecutionException {
        ImmutableList<Class<?>> superTypes = superTypeCache.get(clazz);
        ImmutableList.Builder<Method> methods = ImmutableList.builder();
        superTypes
                .stream()
                .map(Class::getMethods)
                .flatMap(Stream::of)
                .filter(m -> m.isAnnotationPresent(Subscribe.class))
                .filter(m -> !m.isSynthetic())
                .filter(m -> m.getParameterTypes().length == 1)
                .forEach(methods::add);
        return methods.build();
    }

    /**
     * @return all the supertypes of {@code clazz} excluding Object.class.
     */
    private ImmutableList<Class<?>> getSupertypes(Class<?> clazz) {
        ImmutableList.Builder<Class<?>> superTypeBuilder = ImmutableList.builder();
        for (Class<?> curClazz = clazz; curClazz != Object.class; curClazz = curClazz.getSuperclass()) {
            superTypeBuilder.add(curClazz);
        }
        return superTypeBuilder.build();
    }
}
