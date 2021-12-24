package com.ctrip.framework.drc.fetcher.resource.condition;

import com.ctrip.framework.drc.core.driver.binlog.LogEventCallBack;

/**
 * @Author limingdong
 * @create 2020/11/27
 */
public interface ListenableDirectMemory extends DirectMemory {

    long get();

    void addListener(LogEventCallBack listener);
}
