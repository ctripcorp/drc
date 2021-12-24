package com.ctrip.framework.drc.console.monitor.healthcheck.task;

import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.xpipe.api.endpoint.Endpoint;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2019-12-25
 */
public class ExecutedGtidQueryTask extends AbstractQueryTask<String> {

    public ExecutedGtidQueryTask(Endpoint master) {
        super(master);
    }

    @Override
    protected String doQuery() {
        return MySqlUtils.getExecutedGtid(master);
    }

}
