import com.trident.load_balancer.DiskLog
import spock.lang.Specification

import java.time.Duration
import java.time.temporal.ChronoUnit

class LogTest extends Specification {
    def mySegmentFactory;
    def DiskLog log = DiskLog
            .builder()
            .segmentFactory(mySegmentFactory)
            .compactionInterval(Duration.of(5, ChronoUnit.SECONDS))
            .build();



    def 'Appending a single log' () {
            Future<LogAppendAck> ack = log.append("hello", 25);
        then:
            assertThat(ack.appended(), is(true));
    }
}