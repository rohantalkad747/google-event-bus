package com.trident.load_balancer;

import com.google.common.collect.Maps;
import com.trident.load_balancer.DiskLog.Record;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.SerializationUtils;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Data
public class Segment<V> {

    @Data
    @AllArgsConstructor
    private static class ByteOffset {
        private int length;
        private long offset;
    }

    private final long maxSizeBytes;

    private final Path segPath;

    private final Map<String, ByteOffset> offsetTable = Maps.newLinkedHashMap();

    private volatile long currentOffset = 0;

    private volatile boolean writable = false;

    private volatile double currentSizeBytes = 0;

    public Segment(long maxSizeBytes, Path segPath) {
        this.maxSizeBytes = maxSizeBytes;
        this.segPath= segPath;
    }

    public Iterator<Record<V>> getRecords() {
        return null;
    }

    public void writeBytes(Path path, byte[] bytes) throws IOException {
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

    private synchronized boolean appendRecordInBytes(Record<V> record) {
        if (!writable) {
            return false;
        }
        byte[] bytes = SerializationUtils.serialize(record);
        if (bytes == null) {
            return false;
        }
        long newOffset = currentOffset + bytes.length;
        if (newOffset >= maxSizeBytes) {
            markNotWritable();
            return false;
        }
        return tryWrite(record.getKey(), bytes, newOffset);
    }

    private boolean tryWrite(String key, byte[] bytes, long newOffset) {
        try {
            writeBytes(segPath, bytes);
            updateOffset(key, newOffset);
            return true;
        } catch (IOException e) {
            log.error("IOException thrown while trying to write bytes", e);
            return false;
        }
    }

    private void updateOffset(String key, long newOffset) {
        int totalRecordLength = (int) (newOffset - currentOffset - 1);
        ByteOffset offset = new ByteOffset(totalRecordLength, currentOffset);
        offsetTable.put(key, offset);
        currentOffset = newOffset;
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
        return appendRecordInBytes(
                Record.<V>builder()
                .appendTime(new Date().getTime())
                .key(key)
                .val(val)
                .build()
        );
    }

    public boolean appendRecord(Record<V> val) {
        return appendRecordInBytes(val);
    }

    public Record<V> getRecord(String key) throws IOException {
        ByteOffset offset = offsetTable.get(key);
        byte[] bytes = read(segPath, offset.getLength(), offset.getOffset());
        Object maybeRecord = SerializationUtils.deserialize(bytes);
        if (maybeRecord == null) {
            return null;
        } else if (maybeRecord instanceof Record) {
            return (Record<V>) maybeRecord;
        } else {
            throw new RuntimeException(String.format(
                    "Bytes from %s cannot be de-serialized into a Record!",
                    offset
            ));
        }
    }

    /**
     * Manually make this log not writable.
     */
    public void markNotWritable() {
        writable = false;
    }

    public void deleteBackingFile() {
        if ( ! Files.exists(segPath) ) {
            return;
        }
        int maxTries = 3;
        for (int i = 0; i < maxTries; i++) {
            try {
                Files.delete(segPath);
                return;
            } catch (IOException e) {
                log.error("Encountered an exception while delete backing segment file. Retrying ...", e);
            }
        }
        log.warn("Maximum retries reached. Not retrying segment file deletion.");
    }
}
