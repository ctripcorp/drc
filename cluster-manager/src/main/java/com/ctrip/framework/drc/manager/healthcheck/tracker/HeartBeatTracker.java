package com.ctrip.framework.drc.manager.healthcheck.tracker;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.lifecycle.Lifecycle;

import java.util.List;

/**
 * Created by mingdongli
 * 2019/11/21 下午2:54.
 */
public interface HeartBeatTracker extends Lifecycle {

    interface Heartbeat {

        Endpoint getHeartbeatId();

        int getTimeout();

        boolean isClosing();
    }

    interface HeartbeatExpirer {

        void expire(List<Endpoint> downs);

        String getServerId();
    }

    void createHeartbeat(Endpoint key, int heartbeatTimeout);

    void addHeartbeat(Endpoint id, int to);

    boolean touchHeartbeat(Endpoint heartbeatId, int heartbeatTimeout);

    boolean hasHeartbeat(Endpoint heartbeatId);

    void setHeartbeatClosing(Endpoint heartbeatId);

    void removeHeartbeat(Endpoint heartbeatId);

}
