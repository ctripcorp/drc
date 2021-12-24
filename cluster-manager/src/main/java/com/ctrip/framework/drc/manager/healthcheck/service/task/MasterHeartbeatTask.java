package com.ctrip.framework.drc.manager.healthcheck.service.task;

import com.ctrip.xpipe.api.endpoint.Endpoint;

/**
 * Created by mingdongli
 * 2019/11/21 下午10:46.
 */
public class MasterHeartbeatTask extends AbstractMasterQueryTask<Boolean> {

    public MasterHeartbeatTask(Endpoint endpoint) {
        super(endpoint);
    }

    @Override
    protected Boolean doQuery() {
        return isMaster(master);
    }
}
