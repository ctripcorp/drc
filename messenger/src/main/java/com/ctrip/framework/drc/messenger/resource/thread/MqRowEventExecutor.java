package com.ctrip.framework.drc.messenger.resource.thread;

import com.ctrip.framework.drc.messenger.resource.context.MqTransactionContextResource;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Created by shiruixin
 * 2025/1/22 14:38
 */
public interface MqRowEventExecutor {
    CompletableFuture<Boolean> supplyAsync(Supplier<Boolean> supplier, MqTransactionContextResource.RowSendHandler handler);
    CompletableFuture<Boolean> thenApplyAsync(CompletableFuture<Boolean> future, Function<Boolean,Boolean> fn, MqTransactionContextResource.RowSendHandler handler);
}
