package com.ctrip.framework.drc.replicator.impl.inbound.schema.task;

import com.ctrip.framework.drc.replicator.impl.inbound.schema.task.restore.RestoredMysqldProcess;

/**
 * @Author limingdong
 * @create 2022/10/26
 */
public class MySQLInstanceExisting implements MySQLInstance {

    private RestoredMysqldProcess process;

    public MySQLInstanceExisting(RestoredMysqldProcess process) {
        this.process = process;
    }

    @Override
    public void destroy() {
        process.destroy();
    }
}
