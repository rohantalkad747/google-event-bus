package com.trident.event_bus;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
class DeadEvent {
    private EventBus eventBus;
    private Object event;
}
