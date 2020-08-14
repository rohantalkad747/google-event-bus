package com.trident.load_balancer;

import com.google.common.collect.ImmutableList;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

public class VirtualTime {

    public static void execute(final ImmutableList<Task> tasks, final Executor executor) {
        Map<Long, ImmutableList<Task>> runnablesByExecTime = byExecTime(tasks);
        long clock = 0, taskCount = 0;
        final long numTasks = tasks.size();
        ImmutableList<Task> tasksToExecute;
        while (taskCount != numTasks) {
            if ((tasksToExecute = runnablesByExecTime.get(clock)) != null) {
                for (Task task : tasksToExecute) {
                    executor.execute(task.getRunnable());
                    taskCount++;
                }
            }
        }
    }

    private static Map<Long, ImmutableList<Task>> byExecTime(ImmutableList<Task> runnables) {
        return runnables
                .stream()
                .collect(
                        Collectors.groupingBy(
                                Task::getTimeoutMs,
                                ImmutableList.toImmutableList()
                        ));
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
