package com.ctrip.framework.drc.messenger.resource.thread;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;

/**
 * Created by shiruixin
 * 2025/1/22 14:38
 */
public interface MqRowEventExecutor {
    Future<Boolean> submit(Callable<Boolean> callable);
}
