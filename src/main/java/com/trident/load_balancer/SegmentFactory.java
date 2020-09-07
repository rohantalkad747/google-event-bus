package com.trident.load_balancer;

import lombok.AllArgsConstructor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

@AllArgsConstructor
public class SegmentFactory<V> {
    private final AtomicInteger currentSegment = new AtomicInteger(1);
    private final long maxSizeBytes;
    private final Path parentSegmentPath;


    public Segment<V> newInstance() throws IOException {
        int segNumber = currentSegment.getAndIncrement();
        String segName = String.format("segment-%d.dat", segNumber);
        Path newSegPath = parentSegmentPath.resolve(segName);
        Files.createFile(newSegPath);
        return new Segment<>(maxSizeBytes, newSegPath);
    }


}
