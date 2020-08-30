package com.trident.load_balancer;

import com.google.common.collect.Maps;
import com.trident.load_balancer.DiskLog.Record;
import lombok.Data;
import org.springframework.util.SerializationUtils;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

@Data
public class Segment<V extends Serializable> {

    private final Path segmentPath;

    private final long maxSizeBytes;

    private final Path segPath;

    private final Map<String, Integer> offsetTable = Maps.newLinkedHashMap();

    private volatile long currentOffset;

    private AtomicBoolean writable = new AtomicBoolean();

    private double currentSizeBytes;

    public boolean containsKey(String key) {
        return false;
    }

    public Iterator<Record<V>> getRecords() {

    }

    public void write(Path path, byte[] bytes) throws IOException {
        Files.write(path, bytes, StandardOpenOption.APPEND);
    }

    public byte[] read(Path path, int numBytes, long offset) throws IOException {
        ByteBuffer bb = ByteBuffer.allocate(numBytes);
        try (FileChannel ch = FileChannel.open(path)) {
            ch.read(bb, offset);
            return bb.array();
        }
    }

    private byte[] concat(byte[] l, byte[] r) {
        byte[] combined = new byte[l.length + r.length];
        int i = 0;
        for (int j = 0; i < l.length; i++, j++) {
            combined[i] = l[j];
        }
        for (int j = 0; j < r.length; i++, j++) {
            combined[i] = r[j];
        }
        return combined;
    }

    private synchronized boolean tryAppend(Record<V> record) {
        byte[] bytes = SerializationUtils.serialize(record);
        if (bytes == null) {
            return false;
        }
        byte[] lengthBytes = getLengthBytes(bytes);
        long newOffset = currentOffset + lengthBytes.length + bytes.length;
        if (writable.get() && newOffset < maxSizeBytes) {
            try {
                byte[] lengthAndRecordBytes = concat(lengthBytes, bytes);
                write(segmentPath, lengthAndRecordBytes);
                currentOffset = newOffset;
                return true;
            } catch (IOException e) {
                return false;
            }
        }
    }

    private byte[] getLengthBytes(byte[] bytes) {
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putInt(bytes.length);
        return bb.array();
    }

    /**
     * @return true if this value was appended. False indicates that the log is not writable.
     */
    public boolean appendValue(String key, V val) {
        return tryAppend(
                Record.<V>builder()
                .appendTime(new Date().getTime())
                .key(key)
                .val(val).
                build()
        );
    }

    public boolean appendRecord(Record<V> val) {
        return tryAppend(val);
    }

    public Record<V> getRecord() {

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
