package com.trident.load_balancer;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.lang.reflect.Method;

@Data
@AllArgsConstructor
@Builder
public class SubscriberExceptionContext {
    private Object event;
    private Object target;
    private Method method;
}
