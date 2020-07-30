package com.trident.load_balancer;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Instant;

import static com.trident.load_balancer.Component.CPU;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Heartbeat {
    private double timeEpochMs;
    private double ramUsage;
    private double cpuUsage;
    private int connections;

    public Heartbeat nextHeartbeat() throws IOException, InterruptedException {
        return Heartbeat
                .builder()
                .timeEpochMs(Instant.now().toEpochMilli())
                .ramUsage(calcRamUsage())
                .cpuUsage(calcCpuUsage())
                .build();
    }

    private Process getUsageProcess(Component component) throws IOException {
        String command;
        switch (component) {
            case CPU:
                command = "top";
                break;
            case RAM:
                command = "cat /proc/meminfo";
                break;
            default:
                throw new IllegalArgumentException();
        }
        return Runtime.getRuntime().exec(command);
    }

    int getNumber(String s) {
        int strLen = s.length();
        int i = 0, j;
        while (i < strLen && !Character.isDigit(s.charAt(i++)));
        j = i;
        while (j < strLen && s.charAt(j++) != ' ');
        return Integer.parseInt(s.substring(i - 1, j));
    }

    private double calcRamUsage() throws IOException, InterruptedException {
        Process pr = getUsageProcess(CPU);
        BufferedReader in = new BufferedReader(new InputStreamReader(pr.getInputStream()));
        pr.waitFor();
        int memTotal = getNumber(in.readLine());
        int memFree = getNumber(in.readLine());
        return 1 - ((double) memFree / memTotal);
    }

    private double calcCpuUsage() throws IOException, InterruptedException {
        String cpuStatsRow = getCpuStatsRow();
        return 1.0 - getPercentIdle(cpuStatsRow);
    }

    private double getPercentIdle(String cpuStatsRow) {
        int strLen = cpuStatsRow.length();
        int TOP_CPU_STATS_STARTING_INDEX = 9;
        for (int i = TOP_CPU_STATS_STARTING_INDEX; i < strLen - 1; i++)
            if (isIdColumn(cpuStatsRow, i))
                return extractIdlePercentageFromSegment(cpuStatsRow, i);
        throw new RuntimeException("Could not find percent idle time!");
    }

    private boolean isIdColumn(String cpuStatsRow, int i) {
        return cpuStatsRow.charAt(i) == 'i' && cpuStatsRow.charAt(i + 1) == 'd';
    }

    private double extractIdlePercentageFromSegment(String cpuStatsRow, int i) {
        int startingIndex = i - 2;
        int endIndex = startingIndex;
        while (cpuStatsRow.charAt(startingIndex - 1) != ' ')
            --startingIndex;
        return Double.parseDouble(cpuStatsRow.substring(startingIndex, endIndex + 1));
    }

    private String getCpuStatsRow() throws IOException, InterruptedException {
        BufferedReader in = getCpuStatsProcessOutput();
        skipOverFirstThreeRows(in);
        return in.readLine();
    }

    private BufferedReader getCpuStatsProcessOutput() throws IOException, InterruptedException {
        Process pr = getUsageProcess(CPU);
        BufferedReader in = new BufferedReader(new InputStreamReader(pr.getInputStream()));
        pr.waitFor();
        return in;
    }

    private void skipOverFirstThreeRows(BufferedReader in) throws IOException {
        for (int i = 0; i < 3; i++)
            in.readLine();
    }
}
