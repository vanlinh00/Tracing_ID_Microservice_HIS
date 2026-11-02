package com.hospital.common;

import io.micrometer.context.ContextSnapshot;
import org.springframework.core.task.TaskDecorator;

/**
 * TaskDecorator to bridge SLF4J MDC and Micrometer Trace Context across ThreadPools,
 * @Async executions, CompletableFutures, and Project Loom Virtual Threads.
 *
 * Usage:
 * executor.setTaskDecorator(new ContextPropagatingTaskDecorator());
 */
public class ContextPropagatingTaskDecorator implements TaskDecorator {

    @Override
    public Runnable decorate(Runnable runnable) {
        // Takes a snapshot of all ThreadLocal values (MDC, Observation, SecurityContext)
        // on the calling thread, and wraps the target Runnable to apply and revert
        // the snapshot when the worker thread executes.
        return ContextSnapshot.captureAll().wrap(runnable);
    }
}
