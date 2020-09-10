package com.trident.load_balancer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.AllArgsConstructor;
import lombok.Builder;
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
import java.time.Duration;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class MessageLog<V extends Serializable> {

    private final Segment.SegmentFactory<V> segmentFactory;

    private final AtomicInteger activeSegIndex = new AtomicInteger();

    private final List<Segment<V>> segments = Lists.newArrayList();

    public MessageLog(Segment.SegmentFactory<V> segmentFactory, Duration compactionInterval) throws IOException {
        this.segmentFactory = segmentFactory;

        long compactionIntervalMs = compactionInterval.toMillis();

        scheduleCompaction(compactionIntervalMs);

        segments.add(segmentFactory.newInstance());
    }

    private void scheduleCompaction(long compactionIntervalMs) {
        ScheduledExecutorService compaction = Executors.newSingleThreadScheduledExecutor();
        compaction.scheduleAtFixedRate(
                this::performCompaction,
                compactionIntervalMs,
                compactionIntervalMs,
                TimeUnit.MILLISECONDS
        );
    }

    private Segment<V> nextSeg() {
        return moreSegments() ? segments.get(activeSegIndex.incrementAndGet()) : null;
    }

    public Record<V> get(String key) {
        Record<V> v;
        for (Segment<V> segment : Lists.reverse(segments)) {
            try {
                if ((v = segment.get(key)) != null) {
                    return v;
                }
            } catch (IOException e) {
                return null;
            }
        }
        return null;
    }

    /**
     * Appends the given key-value pair to the log synchronously.
     */
    public synchronized void append(String key, V val) throws IOException {
        Segment<V> maybeWritableSegment = segments.get(activeSegIndex.get());
        if (!tryAppendToAnExistingSegment(key, val, maybeWritableSegment)) {
            appendToNewSegment(key, val);
        }
    }

    private boolean tryAppendToAnExistingSegment(String key, V val, Segment<V> maybeWritableSegment) {
        do {
            if (maybeWritableSegment.appendValue(key, val)) {
                return true;
            }
            maybeWritableSegment = nextSeg();
        } while (maybeWritableSegment != null);
        return false;
    }

    private boolean moreSegments() {
        return activeSegIndex.get() + 1 < segments.size();
    }

    private void appendToNewSegment(String key, V val) throws IOException {
        Segment<V> newSegment = segmentFactory.newInstance();
        segments.add(newSegment);
        activeSegIndex.incrementAndGet();
        newSegment.appendValue(key, val);
    }

    private synchronized void performCompaction() {
        Map<String, Record<V>> recordMap = loadRecords();
        try {
            mergeSegmentRecords(recordMap);
        } catch (IOException e) {
            log.warn("IOException while trying to merge ... While try to delete segments anyway.");
        }
        deleteBackingSegments();
    }

    private void deleteBackingSegments() {
        for (Segment<V> segment : segments) {
            segment.deleteBackingFile();
        }
    }

    private void mergeSegmentRecords(Map<String, Record<V>> recordMap) throws IOException {
        Segment<V> currentSegment = initializeSegments();
        for (Record<V> record : recordMap.values()) {
            if (!record.isTombstone() && !currentSegment.appendRecord(record)) {
                currentSegment = segmentFactory.newInstance();
                segments.add(currentSegment);
            }
        }
        activeSegIndex.set(segments.size() - 1);
    }

    private Segment<V> initializeSegments() throws IOException {
        Segment<V> currentSegment = segmentFactory.newInstance();
        segments.clear();
        segments.add(currentSegment);
        return currentSegment;
    }

    private Map<String, Record<V>> loadRecords() {
        Map<String, Record<V>> recordMap = Maps.newHashMap();
        for (Segment<V> segment : segments) {
            Iterator<Record<V>> records = segment.getRecords();
            while (records.hasNext()) {
                Record<V> curRecord = records.next();
                Record<V> maybePreviousRecord = recordMap.get(curRecord.getKey());
                if (isLatestRecord(curRecord, maybePreviousRecord)) {
                    recordMap.put(curRecord.getKey(), curRecord);
                }
            }
            segment.markNotWritable();
        }
        return recordMap;
    }

    private boolean isLatestRecord(Record<V> curRecord, Record<V> maybePreviousRecord) {
        return maybePreviousRecord == null || curRecord.getAppendTime() > maybePreviousRecord.getAppendTime();
    }

    @Data
    @Builder
    @AllArgsConstructor
    public static class Record<V extends Serializable> implements Serializable {
        private final String key;

        private final long appendTime;

        private V val;

        /**
         * @return true if this the k-v pair is to be deleted.
         */
        public boolean isTombstone() {
            return val == null;
        }
    }

    @Slf4j
    @Data
    public static class Segment<V extends Serializable> {

        private final long maxSizeBytes;
        private final Path segPath;
        private final Map<String, ByteOffset> offsetTable = Maps.newLinkedHashMap();
        private volatile long currentOffset = 0;
        private volatile boolean writable = true;
        private volatile double currentSizeBytes = 0;

        public Segment(long maxSizeBytes, Path segPath) {
            this.maxSizeBytes = maxSizeBytes;
            this.segPath = segPath;
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

        private boolean appendRecordInBytes(Record<V> record) {
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
            int totalRecordLength = (int) (newOffset - currentOffset);
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

        public Record<V> get(String key) throws IOException {
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
            if (!Files.exists(segPath)) {
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

        @Data
        @AllArgsConstructor
        private static class ByteOffset {
            private int length;
            private long offset;
        }

        public static class SegmentFactory<V extends Serializable> {
            private final AtomicInteger currentSegment = new AtomicInteger(1);
            private final long maxSizeBytes;
            private final Path parentSegmentPath;

            public SegmentFactory(long maxSizeBytes, Path parentSegmentPath) throws IOException {
                this.maxSizeBytes = maxSizeBytes;
                if (!Files.exists(parentSegmentPath)) {
                    Files.createDirectories(parentSegmentPath);
                }
                this.parentSegmentPath = parentSegmentPath;
            }


            public Segment<V> newInstance() throws IOException {
                int segNumber = currentSegment.getAndIncrement();
                String segName = String.format("segment-%d.dat", segNumber);
                Path newSegPath = parentSegmentPath.resolve(segName);
                if (Files.exists(newSegPath)) {
                    Files.delete(newSegPath);
                }
                Files.createFile(newSegPath);
                return new Segment<>(maxSizeBytes, newSegPath);
            }


        }
    }
}
