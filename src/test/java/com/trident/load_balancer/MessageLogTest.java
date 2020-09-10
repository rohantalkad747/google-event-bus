package com.trident.load_balancer;


import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.temporal.ChronoUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class MessageLogTest {

    @Test
    void test() throws IOException {
        MessageLog<String> logging = new MessageLog<>(new MessageLog.Segment.SegmentFactory<>(120_000, Path.of("test_logs")),
                Duration.of(2, ChronoUnit.SECONDS));
        logging.append("hello", "value");
        MessageLog.Record<String> value = logging.get("hello");
        assertThat(value.getVal(), is("value"));
    }

}