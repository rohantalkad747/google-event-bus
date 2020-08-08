package com.trident.load_balancer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.net.URI;

/**
 * This class is the Heartbeat client for this VM. Its role is to send heartbeats, via HTTP,
 * to the load balancer VM monitor. If an error occurs during the heartbeat information retrieval process,
 * no heartbeat is sent. The monitor has built-in logic to handle anomalies such as clock drift, lack of heartbeats,
 * garbage information, etc. so we don't handle that here.
 */
@Slf4j
public class HeartbeatClient {
    private static final int HB_PERIOD_MS = 1;
    private final TaskScheduler sendHeartbeatJob;
    private final RestTemplate restTemplate;

    HeartbeatClient(HeartbeatSource heartbeatSource, RestTemplate restTemplate, URI leaderURI) {
        this.restTemplate = restTemplate;
        sendHeartbeatJob = new TaskScheduler(sendHbJob(heartbeatSource, leaderURI));
        sendHeartbeatJob.start(HB_PERIOD_MS);
    }

    private Runnable sendHbJob(HeartbeatSource heartbeatSource, URI leaderURI) {
        return () -> {
            try {
                sendHb(heartbeatSource, leaderURI);
            } catch (InterruptedException e) {
                log.error("Thread interrupted while trying to send heartbeat to LB", e);
                Thread.currentThread().interrupt();
            } catch (IOException e) {
                log.error("IO operation failed while trying to send heartbeat to LB", e);
            }
        };
    }

    void shutdown() {
        sendHeartbeatJob.stop();
    }

    private void sendHb(HeartbeatSource heartbeatSource, URI leaderURI) throws IOException, InterruptedException {
        Heartbeat heartbeat = heartbeatSource.beat();
        restTemplate.postForObject(leaderURI, heartbeat, Heartbeat.class);
    }
}
