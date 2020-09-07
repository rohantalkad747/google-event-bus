package com.trident.load_balancer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class DiskLog<V> {

    private final SegmentFactory<V> segmentFactory;

    private final AtomicInteger activeSegIndex = new AtomicInteger();

    private final List<Segment<V>> segments = Lists.newArrayList();

    public DiskLog(SegmentFactory<V> segmentFactory, Duration compactionInterval) {
        this.segmentFactory = segmentFactory;

        long compactionIntervalMs = compactionInterval.get(ChronoUnit.MILLIS);

        scheduleCompaction(compactionIntervalMs);
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

    Segment<V> nextSeg() {
        return segments.get(activeSegIndex.incrementAndGet());
    }

    /**
     * Appends the given key-value pair to the log synchronously.
     */
    public void append(String key, V val) throws IOException {
        Segment<V> maybeWritableSegment = segments.get(activeSegIndex.get());
        boolean appended = tryAppendToAnExistingSegment(key, val, maybeWritableSegment);
        if (!appended) {
            appendToNewSegment(key, val);
        }
    }

    private boolean tryAppendToAnExistingSegment(String key, V val, Segment<V> maybeWritableSegment) {
        for ( ; moreSegments(); maybeWritableSegment = nextSeg()) {
            if (maybeWritableSegment.appendValue(key, val)) {
                return true;
            }
        }
        return false;
    }

    private boolean moreSegments() {
        return activeSegIndex.get() < segments.size();
    }

    private void appendToNewSegment(String key, V val) throws IOException {
        Segment<V> newSegment = segmentFactory.newInstance();
        segments.add(newSegment);
        activeSegIndex.incrementAndGet();
        newSegment.appendValue(key, val);
    }

    private void performCompaction() {
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

    @NonNull
    private Map<String, Record<V>> loadRecords()
    {
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
    public static class Record<V> {
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
}
