package com.ctrip.framework.drc.fetcher.system;

import com.ctrip.framework.drc.fetcher.event.transaction.Traceable;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public abstract class TaskQueueActivity<T, U> extends AbstractLoopActivity implements TaskActivity<T, U> {

    private static class TrackedTask<T> {
        T task;
        long time;

        TrackedTask(T task) {
            this.task = task;
            this.time = System.currentTimeMillis();
        }

        T unwrap() {
            return task;
        }
    }

    private TrackedTask<T> wrap(T task) {
        return new TrackedTask<>(task);
    }

    public int queueSize() {
        return Integer.MAX_VALUE;
    }

    private LinkedBlockingQueue<TrackedTask<T>> tasks = new LinkedBlockingQueue<>(queueSize());

    protected MetricReporter getMetricReporter() {
        return MetricReporter.DEFAULT;
    }

    @Override
    public boolean trySubmit(T task) {
        return tasks.offer(wrap(task));
    }

    @Override
    public void waitSubmit(T task) throws InterruptedException {
        tasks.put(wrap(task));
    }

    @Override
    public boolean waitSubmit(T task, long time, TimeUnit unit) throws InterruptedException {
        return tasks.offer(wrap(task), time, unit);
    }

    protected T next() throws InterruptedException {
        TrackedTask<T> next = tasks.take();
        logger.error("take one");
        getMetricReporter().report(toString() + ".wait.delay", null, System.currentTimeMillis() - next.time);
        return next.unwrap();
    }

    protected boolean hasNext() {
        return !tasks.isEmpty();
    }

    public abstract T doTask(T task) throws InterruptedException;

    protected T retry(T task) {
        return task;
    }

    protected T retry(T task, long time, TimeUnit unit) throws InterruptedException {
        unit.sleep(time);
        return task;
    }

    private T lastTask;
    private int count;
    private long interval;

    @FunctionalInterface
    protected interface callback<T> {
        T execute(T task) throws InterruptedException;
    }

    protected T retry(T task, TimeUnit unit, long base, long delta, long max, int limit, callback<T> callback) throws InterruptedException {
        if (task == lastTask) {
            interval += delta; if (interval > max) interval = max;
            count += 1;
        } else {
            lastTask = task;
            interval = base;
            count = 0;
        }
        if (count >= limit) {
            return callback.execute(task);
        }
        unit.sleep(interval);
        return task;
    }

    protected T finish(T task) {
        if ((task instanceof AutoCloseable)) {
            try {
                ((AutoCloseable) task).close();
            } catch (Throwable t) {
                logger.error("UNLIKELY - fail to close task: ", t);
            }
        }
        return null;
    }

    protected T hand(U task) throws InterruptedException {
        select(task);
        latter().waitSubmit(task);
        return null;
    }

    @Override
    public void loop() throws InterruptedException {
        T task = next();
        long start = System.currentTimeMillis();
        while(task != null) {
            try {
                task = doTask(task);
            } catch (InterruptedException e) {
                //QUIT
                finish(task);
                while(!tasks.isEmpty()) {
                    task = next();
                    finish(task);
                }
                throw e;
            } catch (Throwable e) {
                //UNLIKELY
                String identifier = task.getClass().getSimpleName();
                if (task instanceof Traceable) {
                    identifier = ((Traceable) task).identifier();
                }
                logger.error("{}({}) - UNLIKELY: ", getClass().getSimpleName(), identifier, e);
                if (!isStarted()) {
                    finish(task);
                    while(!tasks.isEmpty()) {
                        task = next();
                        finish(task);
                    }
                    task = null;
                }
                Thread.sleep(2000);
            }
        }
        getMetricReporter().report(toString() + ".execute.delay", null, System.currentTimeMillis() - start);
    }

    private final List<TaskActivity<U, ?>> alters = Lists.newArrayList();
    private int current = 0;

    protected void select(U task) {
        current = (current + 1) % alters.size();
    }

    protected TaskActivity<U, ?> latter() {
        return alters.get(current);
    }

    public <V> TaskActivity<U, V> link(TaskActivity<U, V> latter) {
        alters.add(latter);
        return latter;
    }

}
