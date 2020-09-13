import com.trident.load_balancer.VirtualTime
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean

import static org.hamcrest.MatcherAssert.assertThat
import static org.hamcrest.Matchers.contains
import static org.junit.jupiter.api.Assertions.assertTrue


class VirtualTimeTest extends Specification {
    def virtualTime = new VirtualTime()


    def 'Run a single task'() {
        def didRun = new AtomicBoolean()
        given:
        def conditions = new PollingConditions(timeout: 5)
        def runnable = () -> { didRun.set(true) }
        def inAFewSeconds = Instant.ofEpochMilli(System.currentTimeMillis() + 3000)
        def tasks = [new VirtualTime.Task(inAFewSeconds, runnable)]
        when:
        virtualTime.execute(tasks)
        then:
        conditions.eventually {
            assertTrue(didRun.get())
        }
    }

    def 'Running a chain of tasks'() {
        given:
        def conditions = new PollingConditions(timeout: 5)

        def callStack = []

        def runnableOne = () -> { callStack << 1 }
        def runnableTwo = () -> { callStack << 2 }
        def runnableThree = () -> { callStack << 3 }

        def currentTimeEpochMs = System.currentTimeMillis() // Keep a small buffer for below steps
        def inOneSecond = Instant.ofEpochMilli(currentTimeEpochMs + 3000)
        def inTwoSeconds = Instant.ofEpochMilli(currentTimeEpochMs + 4000)
        def inThreeSeconds = Instant.ofEpochMilli(currentTimeEpochMs + 5000)

        def tasks = [
                new VirtualTime.Task(inOneSecond, runnableOne),
                new VirtualTime.Task(inTwoSeconds, runnableTwo),
                new VirtualTime.Task(inThreeSeconds, runnableThree)
        ]
        when:
        virtualTime.execute(tasks)
        then:
        conditions.eventually {
            assertThat(callStack, contains(1, 2, 3))
        }
    }
}