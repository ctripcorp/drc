package com.ctrip.framework.drc.manager.healthcheck;

import com.ctrip.xpipe.api.endpoint.Endpoint;

/**
 * Created by mingdongli
 * 2019/11/21 下午2:56.
 */
public class MySQLHeartBeatContext implements HeartBeatContext {

    @Override
    public boolean accept(Endpoint ip) {
        return true;
    }

    @Override
    public int getLease() {
        return 15 * 1000;
    }

    @Override
    public int getInterval() {
        return 5 * 1000;
    }

}