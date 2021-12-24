package com.ctrip.framework.drc.replicator.impl.inbound.schema.task;

import com.ctrip.framework.drc.core.driver.binlog.manager.task.NamedCallable;
import com.wix.mysql.EmbeddedMysql;
import ctrip.framework.drc.mysql.EmbeddedDb;

/**
 * @Author limingdong
 * @create 2021/4/7
 */
public class DbInitTask implements NamedCallable<EmbeddedMysql> {

    private int port;

    public DbInitTask(int port) {
        this.port = port;
    }

    @Override
    public EmbeddedMysql call() {
        return new EmbeddedDb().mysqlServer(port);
    }
}
