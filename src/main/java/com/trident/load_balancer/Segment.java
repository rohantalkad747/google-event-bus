package com.trident.load_balancer;

import com.google.common.collect.Maps;
import lombok.Data;

import java.io.File;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;

@Data
public class Segment<V extends Serializable> {

    private final int maxSizeMb;

    private int currentSize;

    private final File segFile;

    private int offset;

    private final Map<String, Integer> offsetTable = Maps.newLinkedHashMap();

    public boolean containsKey(String key) {
        return false;
    }

    public Iterator<DiskLog.Record<V>> getRecords() {

    }

    public void appendValue(String key, V val) {

    }

    public void appendRecord(DiskLog.Record<V> val) {

    }

    public boolean writable() {
        return false;
    }

    public void freeze() {
    }

    public void deleteBackingFile() {

    }
}
