package com.trident.load_balancer;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
class DeadEvent {
    private EventBus eventBus;
    private Object event;
}
