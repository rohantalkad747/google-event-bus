package com.trident.load_balancer;

import com.google.common.collect.Maps;
import lombok.Data;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

@Data
public class Segment<V extends Serializable> {

    synchronized void write(Path path, byte[] bytes) throws IOException {
        Files.write(path, bytes, StandardOpenOption.APPEND);
    }

    synchronized void read(Path path, int numBytes, long offset) throws IOException {
        ByteBuffer bb = ByteBuffer.allocate(numBytes);
        try (FileChannel ch = FileChannel.open(path)) {
            ch.read(bb, offset);
        }
    }

    private final int maxSizeMb;

    private final File segFile;

    private final Map<String, Integer> offsetTable = Maps.newLinkedHashMap();

    private AtomicBoolean writable = new AtomicBoolean();

    private double currentSizeMb;

    public boolean containsKey(String key) {
        return false;
    }

    public Iterator<DiskLog.Record<V>> getRecords() {

    }

    /**
     * @return true if this value was appended. False indicates that the log is not writable.
     */
    public boolean appendValue(String key, V val) {
    }

    public boolean appendRecord(DiskLog.Record<V> val) {

    }

    public boolean writable() {
        return false;
    }

    /**
     * Manually make this log not writable.
     */
    public void markNotWritable() {
        writable.set(false);
    }

    public void deleteBackingFile() {

    }
}
