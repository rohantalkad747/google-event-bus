package com.trident.load_balancer;

import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;

import java.net.URI;
import java.util.Arrays;
import java.util.Map;

import static java.lang.System.exit;
import static java.lang.System.out;

@Slf4j
public class HeartbeatClient {
    private static final String LEADER_URI_FLAG = "--leader-uri";
    private static final String HEARTBEAT_PERIOD_FLAG = "--heartbeat-period";
    private static final int MINIMUM_HB_PERIOD_MS = 500;
    private static final int MAXIMUM_HB_PERIOD_MS = 60000;
    private final WebClient webClient;

    HeartbeatClient(
            WebClient webClient,
            HeartbeatSource heartbeatSource,
            URI leaderURI,
            int heartbeatMs
    ) {
        this.webClient = webClient;
        TaskScheduler sendHeartbeatJob = new TaskScheduler(sendHbJob(heartbeatSource, leaderURI));
        sendHeartbeatJob.start(heartbeatMs);
    }

    private static Map<String, String> getParameterToValue(String[] args) {
        Map<String, String> parameterValueMap = Maps.newHashMap();
        for (int i = 0; i < args.length - 1; i += 2)
            appendFlagValue(args, parameterValueMap, i);
        return parameterValueMap;
    }

    private static void appendFlagValue(String[] args, Map<String, String> parameterValueMap, int i) {
        parameterValueMap.put(args[i], args[i + 1]);
    }

    public static void main(String[] args) {
        checkIfShouldPrintUsage(args);
        Map<String, String> parameterToValue = getParameterToValue(args);
        int heartbeatPeriod = Integer.parseInt(parameterToValue.get(HEARTBEAT_PERIOD_FLAG));
        checkHeartbeatPeriod(heartbeatPeriod);
        URI leaderURI = URI.create(parameterToValue.get(LEADER_URI_FLAG));
        HeartbeatSource heartbeatSource = new HeartbeatSource();
        WebClient client = WebClient.create();
        new HeartbeatClient(client, heartbeatSource, leaderURI, heartbeatPeriod);
    }

    private static void checkHeartbeatPeriod(int heartbeatPeriod) {
        if (heartbeatPeriod < MINIMUM_HB_PERIOD_MS || heartbeatPeriod > MAXIMUM_HB_PERIOD_MS) {
            printHeartbeatPeriodBounds();
            exit(1);
        }
    }

    private static void printHeartbeatPeriodBounds() {
        out.println(
                String.format(
                        "Heartbeat period time must be between %s ms and %s ms",
                        MINIMUM_HB_PERIOD_MS,
                        MAXIMUM_HB_PERIOD_MS
                )
        );
    }

    private static void checkIfShouldPrintUsage(String[] args) {
        if (shouldPrintUsage(args)) {
            printUsage();
            exit(1);
        }
    }

    private static boolean shouldPrintUsage(String[] args) {
        return emptyInvocation(args) ||
                helpFlagPresent(args[0]) ||
                moreThanExpectedArgs(args) ||
                unexpectedFlag(args);
    }

    private static boolean unexpectedFlag(String[] args) {
        return Arrays
                .stream(args)
                .filter(arg -> arg.startsWith("--"))
                .anyMatch(arg -> !LEADER_URI_FLAG.equals(arg) && !HEARTBEAT_PERIOD_FLAG.equals(arg));
    }

    private static boolean moreThanExpectedArgs(String[] args) {
        return args.length != 4;
    }

    private static boolean emptyInvocation(String[] args) {
        return args.length == 0;
    }

    private static boolean helpFlagPresent(String arg) {
        return arg.equals("--h");
    }

    private static void printUsage() {
        out.println(
                "Trident Load Balancer Usage:\n" +
                        "Flag               Description\n" +
                        "------------------------------------------\n" +
                        "[--h]              To display help\n" +
                        "--leader-uri       The URI to the leader node\n" +
                        "--heartbeat-period The period to send heartbeats (ms)"
        );
    }

    private Runnable sendHbJob(HeartbeatSource heartbeatSource, URI leaderURI) {
        return () -> sendHb(heartbeatSource, leaderURI);
    }

    private void sendHb(HeartbeatSource heartbeatSource, URI leaderURI) {
        Heartbeat heartbeat = heartbeatSource.beat();
        log.info(
                String.format(
                        "Posting heartbeat %s to node at URI: %s",
                        heartbeat,
                        leaderURI)
        );
        webClient
                .post()
                .uri(leaderURI)
                .body(BodyInserters.fromValue(heartbeat))
                .retrieve()
                .bodyToMono(Void.class);
    }
}
