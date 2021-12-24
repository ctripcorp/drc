package com.ctrip.framework.drc.fetcher.resource.condition;

/**
 * @Author limingdong
 * @create 2020/11/26
 */
public interface DirectMemory {

    boolean tryAllocate(long size);

    boolean release(long size);
}
