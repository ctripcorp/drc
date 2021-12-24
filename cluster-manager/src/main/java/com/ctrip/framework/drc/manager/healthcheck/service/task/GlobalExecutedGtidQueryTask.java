package com.ctrip.framework.drc.manager.healthcheck.service.task;

import com.ctrip.xpipe.api.endpoint.Endpoint;

public class GlobalExecutedGtidQueryTask extends ExecutedGtidQueryTask {

    private static final String EXECUTED_GTID = ALI_RDS + "show global variables like \"gtid_executed\";";

    public GlobalExecutedGtidQueryTask(Endpoint master) {
        super(master);
    }

    @Override
    public String getCommand() {
        return EXECUTED_GTID;
    }

    @Override
    public int getIdx() {
        return 2;
    }

    @Override
    protected boolean needDownGrade() {
        return false;
    }
}
