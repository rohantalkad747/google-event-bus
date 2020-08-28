package com.trident.load_balancer;

import java.io.Serializable;

public class SegmentFactory<V extends Serializable> {
    public Segment<V> newInstance() {
        return null;
    }
}
