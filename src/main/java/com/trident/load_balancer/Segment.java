package com.trident.load_balancer;

import com.google.common.collect.Maps;
import lombok.Data;

import java.io.File;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

@Data
public class Segment<V extends Serializable> {

    private final int maxSizeMb;
    private final File segFile;
    private final Map<String, Integer> offsetTable = Maps.newLinkedHashMap();
    private AtomicBoolean writable = new AtomicBoolean();
    private int currentSize;
    private int offset;

    public boolean containsKey(String key) {
        return false;
    }

    public Iterator<DiskLog.Record<V>> getRecords() {

    }

    public synchronized boolean appendValue(String key, V val) {

    }

    public synchronized boolean appendRecord(DiskLog.Record<V> val) {

    }

    public boolean writable() {
        return false;
    }

    /**
     * Manually make this log not writable.
     */
    public void freeze() {
        writable.set(false);
    }

    public void deleteBackingFile() {

    }
}
