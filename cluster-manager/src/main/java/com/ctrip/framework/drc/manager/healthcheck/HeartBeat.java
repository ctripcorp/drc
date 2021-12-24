package com.ctrip.framework.drc.manager.healthcheck;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.lifecycle.Lifecycle;

/**
 * Created by mingdongli
 * 2019/11/21 下午3:11.
 */
public interface HeartBeat extends Lifecycle {

    void pingMaster();

    void pingZone();

    void addServer(Endpoint endpoint);

    void removeServer(Endpoint endpoint);
}
