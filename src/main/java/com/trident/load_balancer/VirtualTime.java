package com.trident.load_balancer;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Single threaded executor that simulates a scheduled task executor.
 */
@Slf4j
public class VirtualTime {
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    private Map<Long, ImmutableList<Task>> partitionByExecTime(List<Task> runnables) {
        return runnables
                .stream()
                .collect(
                        Collectors.groupingBy(
                                Task::getTimeoutMs,
                                ImmutableList.toImmutableList()
                        ));
    }

    private void shutdown() {
       if (!shutdown.get()) {
           executor.shutdown();
           shutdown.set(true);
       } else {
           throw new RuntimeException("VirtualTime instance already shutdown!");
       }
    }

    public void execute(final List<Task> tasks) {
        Preconditions.checkArgument(
                !shutdown.get(),
                "VirtualTime instance already shutdown!"
        );
        Map<Long, ImmutableList<Task>> tasksByExecTime = partitionByExecTime(tasks);
        long clock = System.currentTimeMillis(), taskCount = 0;
        final long numTasks = tasks.size();
        ImmutableList<Task> tasksToExecute;
        while (!shutdown.get() && taskCount != numTasks) {
            if ((tasksToExecute = tasksByExecTime.get(++clock)) != null) {
                for (Task task : tasksToExecute) {
                    executor.execute(task.getRunnable());
                    taskCount++;
                }
            }
        }
        shutdown();
    }

    @Data
    @AllArgsConstructor
    public static class Task {
        private final long timeoutMs;
        private final Runnable runnable;

        public Task(final Instant instant, final Runnable runnable) {
            this.timeoutMs = instant.toEpochMilli();
            this.runnable = runnable;
        }
    }
}
