package com.ctrip.framework.drc.messenger.resource.thread;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Created by shiruixin
 * 2025/1/22 14:38
 */
public interface MqRowEventExecutor {
    Future<Boolean> submit(Callable<Boolean> callable);
    CompletableFuture<Boolean> supplyAsync(Supplier<Boolean> supplier);
    CompletableFuture<Boolean> thenApplyAsync(CompletableFuture<Boolean> future, Function<Boolean,Boolean> fn);
}
