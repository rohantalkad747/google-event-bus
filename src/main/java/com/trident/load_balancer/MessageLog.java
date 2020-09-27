package com.trident.load_balancer;

import com.google.common.collect.ImmutableList;
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
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This message log is simplistic, single-threaded key-value store. Records are appended to a single segment file up
 * until the maximum size of the file is reached, upon which a new segment file is created. A compaction daemon thread
 * triggers the merging of multiple files. During compaction, the latest value associated with a key is taken. Tombstone
 * key-value pairs, with null values, indicate that the key-value pair will be deleted after compaction. One significant
 * limitation is that segment file sizes must be much greater than a single object's size, as there is no wrapping around
 * multiple segment files.
 *
 * @author Rohan Talkad
 */
@Slf4j
public class MessageLog<V extends Serializable> {

    private static final int MAX_ATTEMPTS_AT_DELETING_BACKING_FILE = 3;

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
        new TaskScheduler(this::performCompaction).start(compactionIntervalMs);
    }

    private Segment<V> nextSeg() {
        return moreSegments() ? segments.get(activeSegIndex.incrementAndGet()) : null;
    }

    /**
     * @return the value associated with the given key, if it exists.s
     */
    public synchronized Record<V> get(String key) {
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
        byte[] bytes = bytesOfKeyValuePair(key, val);
        Segment<V> maybeWritableSegment = segments.get(activeSegIndex.get());
        checkIfObjectIsTooLargeToFitIntoSegmentFile(bytes, maybeWritableSegment);
        if (!tryAppendToAnExistingSegment(key, bytes, maybeWritableSegment)) {
            appendToNewSegment(key, val);
        }
    }

    private void checkIfObjectIsTooLargeToFitIntoSegmentFile(byte[] bytes, Segment<V> maybeWritableSegment) {
        if (bytes.length > maybeWritableSegment.maxSizeBytes) {
            throw new RuntimeException("This object cannot fit into a segment file!");
        }
    }

    private byte[] serializeEnsuringNonNullResult(Record<V> rec) {
        byte[] bytes = SerializationUtils.serialize(rec);
        if (bytes == null) {
            throw new RuntimeException("Could not serialize record! " + rec);
        }
        return bytes;
    }

    private byte[] bytesOfKeyValuePair(String key, V val) {
        Record<V> rec = Record.<V>builder()
                .appendTime(new Date().getTime())
                .key(key)
                .val(val)
                .build();
        return serializeEnsuringNonNullResult(rec);
    }

    private boolean tryAppendToAnExistingSegment(String key, byte[] bytes, Segment<V> maybeWritableSegment) {
        do {
            if (maybeWritableSegment.appendValue(key, bytes)) {
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
        byte[] bytes = bytesOfKeyValuePair(key, val);
        newSegment.appendValue(key, bytes);
    }

    private synchronized void performCompaction() {
        List<Segment<V>> oldSegments = Lists.newArrayList(segments);
        try {
            Map<String, Record<V>> recordMap = loadRecords();
            if (mergeSegmentRecords(recordMap)) {
                deleteBackingSegments(oldSegments);
            }
        } catch (IOException e) {
            log.warn("IOException while trying to merge ...", e);
        }
    }

    private void deleteBackingSegments(List<Segment<V>> segmentsToDelete) {
        for (Segment<V> segment : segmentsToDelete) {
            segment.deleteBackingFile();
        }
    }

    private boolean mergeSegmentRecords(Map<String, Record<V>> recordMap) throws IOException {
        if (segmentsCompactable()) {
            log.info("Performing compaction!");
            Segment<V> currentSegment = initializeSegments();
            for (Record<V> record : recordMap.values()) {
                if (!record.isTombstone() && !currentSegment.appendRecordInBytes(record.getKey(), serializeEnsuringNonNullResult(record))) {
                    currentSegment = segmentFactory.newInstance();
                    segments.add(currentSegment);
                }
            }
            activeSegIndex.set(segments.size() - 1);
            return true;
        } else {
            log.info("Not performing compaction since single segment not filled!");
            return false;
        }
    }

    private boolean segmentsCompactable() {
        return segments.size() != 1;
    }

    private Segment<V> initializeSegments() throws IOException {
        Segment<V> currentSegment = segmentFactory.newInstance();
        segments.clear();
        segments.add(currentSegment);
        return currentSegment;
    }

    private Map<String, Record<V>> loadRecords() throws IOException {
        Map<String, Record<V>> recordMap = Maps.newHashMap();
        for (Segment<V> segment : segments) {
            List<Record<V>> records = segment.getRecords();
            if (records != null) {
                for (Record<V> curRecord : records) {
                    Record<V> maybePreviousRecord = recordMap.get(curRecord.getKey());
                    if (isLatestRecord(curRecord, maybePreviousRecord)) {
                        recordMap.put(curRecord.getKey(), curRecord);
                    }
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

        public ImmutableList<Record<V>> getRecords() throws IOException {
            byte[] allBytes = Files.readAllBytes(segPath);
            ImmutableList.Builder<Record<V>> records = ImmutableList.builder();
            for (Entry<String, ByteOffset> offsetEntry : offsetTable.entrySet()) {
                ByteOffset byteOffset = offsetEntry.getValue();
                long offset = byteOffset.getOffset();
                byte[] bytes = Arrays.copyOfRange(allBytes, (int) offset, (int) offset + byteOffset.getLength());
                try {
                    Record<V> rec = (Record<V>) SerializationUtils.deserialize(bytes);
                    if (rec != null) {
                        records.add(rec);
                    }
                } catch (ClassCastException e) {
                    log.warn("Failed to cast a record!");
                }
            }
            return records.build();
        }

        public void writeBytesToSegmentFile(Path path, byte[] bytes) throws IOException {
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

        private boolean appendRecordInBytes(String key, byte[] bytes) {
            if (!writable) {
                log.info(String.format("Segment with path %s is not writable", segPath));
                return false;
            }
            long newOffset = currentOffset + bytes.length;
            if (newOffset > maxSizeBytes) {
                log.trace(String.format("Record with key %s too large for segment %s!", key, segPath));
                return false;
            } else if (newOffset == maxSizeBytes) {
                markNotWritable();
            }
            return tryWrite(key, bytes, newOffset);
        }

        private boolean tryWrite(String key, byte[] bytes, long newOffset) {
            try {
                writeBytesToSegmentFile(segPath, bytes);
                updateSegmentStateVariables(key, newOffset);
                return true;
            } catch (IOException e) {
                log.error("IOException thrown while trying to write bytes", e);
                return false;
            }
        }

        private void updateSegmentStateVariables(String key, long newOffset) {
            int totalRecordLength = (int) (newOffset - currentOffset);
            ByteOffset offset = new ByteOffset(totalRecordLength, currentOffset);
            offsetTable.put(key, offset);
            currentOffset = newOffset;
            currentSizeBytes += totalRecordLength;
        }

        private byte[] getLengthBytes(byte[] bytes) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(bytes.length);
            return bb.array();
        }

        /**
         * @return true if this value was appended. False indicates that the log is not writable.
         */
        public boolean appendValue(String key, byte[] bytes) {
            return appendRecordInBytes(key, bytes);
        }

        public Record<V> get(String key) throws IOException {
            ByteOffset offset = offsetTable.get(key);
            if (offset == null) {
                return null;
            }
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
                log.trace("Backing file does not exist!");
                return;
            }
            for (int i = 0; i < MAX_ATTEMPTS_AT_DELETING_BACKING_FILE; i++) {
                if (tryDelete()) {
                    return;
                }
            }
            log.warn("Maximum retries reached. Not retrying segment file deletion.");
        }

        private boolean tryDelete() {
            try {
                Files.delete(segPath);
                return true;
            } catch (IOException e) {
                log.error("Encountered an exception while delete backing segment file. Retrying ...", e);
            }
            return false;
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
                createSegFile(newSegPath);
                return new Segment<>(maxSizeBytes, newSegPath);
            }

            private void createSegFile(Path newSegPath) throws IOException {
                log.trace("Creating file " + newSegPath);
                if (Files.exists(newSegPath)) {
                    Files.delete(newSegPath);
                }
                Files.createFile(newSegPath);
            }
        }
    }
}
