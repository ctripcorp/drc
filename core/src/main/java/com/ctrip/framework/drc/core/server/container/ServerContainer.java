package com.ctrip.framework.drc.core.server.container;

import com.ctrip.xpipe.api.endpoint.Endpoint;

/**
 * Created by mingdongli
 * 2019/10/11 上午9:33.
 */
public interface ServerContainer<C, S> {

    S addServer(C config);

    Endpoint getUpstreamMaster(String identity);

    void removeServer(String identity, boolean closeLeaderElector);

    S register(String identity, int port);

    S getInfo();
}
