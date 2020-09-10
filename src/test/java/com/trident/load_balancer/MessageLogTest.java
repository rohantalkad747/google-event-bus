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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class MessageLogTest {

    @BeforeEach
    void delete() throws IOException {
        Path test_logs = Path.of("test_logs");
        if (test_logs.toFile().exists()) {
            Files
                    .walk(Path.of("test_logs"))
                    .sorted(Comparator.reverseOrder())
                    .forEach(Unchecked.consumer(Files::delete));
        }
    }

    @Test
    void test() throws IOException, InterruptedException {
        MessageLog<String> logging = new MessageLog<>(
                new MessageLog.Segment.SegmentFactory<>
                (
                        477,
                        Path.of("test_logs")
                ),
                Duration.of(10, ChronoUnit.SECONDS)
        );
        logging.append("hello", "value");
        MessageLog.Record<String> value = logging.get("hello");
        assertThat(value.getVal(), is("value"));

        logging.append("fello", "malue");
        MessageLog.Record<String> malue = logging.get("fello");
        assertThat(malue.getVal(), is("malue"));

        logging.append("mello", "balue");
        MessageLog.Record<String> balue = logging.get("mello");
        assertThat(balue.getVal(), is("balue"));

        logging.append("JELLO", "ball");
        logging.append("asdasdasdas", "wewqaeeawds");
        logging.append("asdare3", "324234das");
    }

    @Test
    void compactionTest() throws IOException, InterruptedException {
        MessageLog<String> logging = new MessageLog<>(
                new MessageLog.Segment.SegmentFactory<>
                        (
                                200_000,
                                Path.of("test_logs")
                        ),
                Duration.of(10, ChronoUnit.SECONDS)
        );
        for (int i = 0; i < 2500; i++) {
            logging.append("asdare" + i, "324234das");
        }
        for (int i = 0; i < 2500; i++) {
            logging.append("asdare" + i, String.format("%s%s%s%s", i, i, i, i));
        }
        Thread.sleep(12_000);

        assertThat(logging.get("asdare2499").getVal(), is("2499249924992499"));
    }




}