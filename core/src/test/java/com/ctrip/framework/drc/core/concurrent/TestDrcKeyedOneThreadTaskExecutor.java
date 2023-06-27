package com.ctrip.framework.drc.core.concurrent;

import java.util.concurrent.Executor;

/**
 * Created by jixinwang on 2023/6/19
 */
public class TestDrcKeyedOneThreadTaskExecutor extends DrcKeyedOneThreadTaskExecutor {
    public TestDrcKeyedOneThreadTaskExecutor(Executor executors) {
        super(executors);
    }

    @Override
    protected int getRetryInterval() {
        return 20;
    }
}
