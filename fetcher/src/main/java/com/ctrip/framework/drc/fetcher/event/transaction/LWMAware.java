package com.ctrip.framework.drc.fetcher.event.transaction;

import com.ctrip.framework.drc.fetcher.resource.condition.LWM;
import com.ctrip.framework.drc.fetcher.resource.condition.LWMPassHandler;

/**
 * @Author Slight
 * Jul 07, 2020
 */
public interface LWMAware {

    void begin(LWM lwm, LWMPassHandler handler) throws InterruptedException;
}
