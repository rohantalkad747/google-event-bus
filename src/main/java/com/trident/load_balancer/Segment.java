package com.trident.load_balancer;

import com.google.common.collect.Maps;
import com.trident.load_balancer.DiskLog.Record;
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
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Data
public class Segment<V extends Serializable> {

    private final long maxSizeBytes;

    private final Path segPath;

    private final Map<String, Long> offsetTable = Maps.newLinkedHashMap();

    private volatile long currentOffset;

    private AtomicBoolean writable = new AtomicBoolean();

    private double currentSizeBytes;

    public Iterator<Record<V>> getRecords() {

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
        byte[] bytes = SerializationUtils.serialize(record);
        if (bytes == null) {
            return false;
        }
        byte[] lengthBytes = getLengthBytes(bytes);
        long newOffset = currentOffset + lengthBytes.length + bytes.length;
        if (!writable.get() || newOffset >= maxSizeBytes) {
            return false;
        }
        return tryWrite(record.getKey(), bytes, lengthBytes, newOffset);
    }

    private boolean tryWrite(String key, byte[] bytes, byte[] lengthBytes, long newOffset) {
        try {
            writeBytes(bytes, lengthBytes);
            updateOffset(key, newOffset);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private void updateOffset(String key, long newOffset) {
        offsetTable.put(key, currentOffset);
        currentOffset = newOffset;
    }

    private void writeBytes(byte[] bytes, byte[] lengthBytes) throws IOException {
        byte[] lengthAndRecordBytes = concat(lengthBytes, bytes);
        writeBytes(segPath, lengthAndRecordBytes);
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
                .val(val).
                build()
        );
    }

    public boolean appendRecord(Record<V> val) {
        return appendRecordInBytes(val);
    }

    public Record<V> getRecord(String key) {
        Long offset = offsetTable.get(key);
        try {
            byte[] length = read(segPath, 32, offset);
            ByteBuffer bb = ByteBuffer.wrap(length);
            int recLength = bb.getInt();
            byte[] recBytes = read(segPath, (int) (offset + 32), recLength);
            Object maybeRecord = SerializationUtils.deserialize(recBytes);
            if (maybeRecord instanceof Record record) {
                return record;
            }
            throw new RuntimeException(String.format(
                    "Bytes from [%s, %s] cannot be de-serialized into a Record!",
                    offset, offset + recLength
            ));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Manually make this log not writable.
     */
    public void markNotWritable() {
        writable.set(false);
    }

    public void deleteBackingFile() {
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
