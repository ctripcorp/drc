package com.ctrip.framework.drc.fetcher.activity.event;

import java.util.concurrent.TimeUnit;

/**
 * @Author limingdong
 * @create 2021/3/25
 */
public interface AcquireActivity {

    boolean tryAcquire(long timeout, TimeUnit unit) throws InterruptedException;
}
