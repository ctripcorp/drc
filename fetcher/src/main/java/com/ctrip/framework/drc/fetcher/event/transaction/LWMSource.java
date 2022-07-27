package com.ctrip.framework.drc.fetcher.event.transaction;

import com.ctrip.framework.drc.fetcher.resource.condition.LWM;

/**
 * @Author Slight
 * Jul 07, 2020
 */
public interface LWMSource {

    void commit(LWM lwm) throws InterruptedException;
}
