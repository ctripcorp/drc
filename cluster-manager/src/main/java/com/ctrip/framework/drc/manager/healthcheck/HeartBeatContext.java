package com.ctrip.framework.drc.manager.healthcheck;

import com.ctrip.xpipe.api.endpoint.Endpoint;

/**
 * Created by mingdongli
 * 2019/11/21 下午2:55.
 */
public interface HeartBeatContext {

    boolean accept(Endpoint ip);

    int getLease();

    int getInterval();

}
