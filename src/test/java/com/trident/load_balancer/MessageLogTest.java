package com.trident.load_balancer;


import org.jooq.lambda.Unchecked;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;

import static java.lang.Thread.sleep;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class MessageLogTest {
    Path TEST_LOGS = Path.of("test_logs");

    @BeforeEach
    void delete() throws IOException {
        if (TEST_LOGS.toFile().exists()) {
            Files
                    .walk(Path.of("test_logs"))
                    .sorted(Comparator.reverseOrder())
                    .forEach(Unchecked.consumer(Files::delete));
        }
    }

    @Test
    void test() throws IOException, InterruptedException {
        // Given this logging config
        MessageLog<String> logging = new MessageLog<>(
                new MessageLog.Segment.SegmentFactory<>
                        (
                                477,
                                Path.of("test_logs")
                        ),
                Duration.of(10, ChronoUnit.SECONDS)
        );

        // When we append the given key-value pairs
        logging.append("hello", "value");
        logging.append("fello", "malue");
        logging.append("mello", "balue");

        // Then the values are present
        MessageLog.Record<String> value = logging.get("hello");
        assertThat(value.getVal(), is("value"));

        MessageLog.Record<String> malue = logging.get("fello");
        assertThat(malue.getVal(), is("malue"));

        MessageLog.Record<String> balue = logging.get("mello");
        assertThat(balue.getVal(), is("balue"));
    }

    @Test
    void compactionTest() throws IOException, InterruptedException {
        // Given this logging config
        MessageLog<String> logging = new MessageLog<>(
                new MessageLog.Segment.SegmentFactory<>
                        (
                                200_000,
                                Path.of("test_logs")
                        ),
                Duration.of(2, ChronoUnit.SECONDS)
        );
        // And a number of K-V writes (some duplicated) such that many log files are produced
        writeUniqueAndRandomKvWrites(logging);
        int beforeCompaction = TEST_LOGS.toFile().listFiles().length;

        // When compaction finishes
        sleep(2_000);
        int afterCompaction = TEST_LOGS.toFile().listFiles().length;

        // Then the segment files should be compacted such that afterCompaction < beforeCompaction
        assertThat(beforeCompaction, is(6));
        assertThat(afterCompaction, is(4));
        // And the values are present
        assertThat(logging.get("asdare1000").getVal(), is("1000"));
        assertThat(logging.get("asdare2499").getVal(), is("2499"));
        // And replacements are also registered
        assertThat(logging.get("asdare2500").getVal(), is("2501"));
        assertThat(logging.get("asdare4999").getVal(), is("5000"));
    }

    private void writeUniqueAndRandomKvWrites(MessageLog<String> logging) throws IOException {
        for (int i = 1000; i < 5000; i++) {
            logging.append("asdare" + i, String.format("%s", i));
        }
        for (int i = 2500; i < 5000; i++) {
            logging.append("asdare" + i, String.format("%s", i + 1));
        }
    }


}