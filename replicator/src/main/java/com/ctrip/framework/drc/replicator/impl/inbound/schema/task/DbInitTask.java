package com.ctrip.framework.drc.replicator.impl.inbound.schema.task;

import com.ctrip.framework.drc.core.driver.binlog.manager.task.NamedCallable;
import com.ctrip.framework.drc.replicator.impl.inbound.schema.MySQLVariablesConfiguration;
import com.wix.mysql.EmbeddedMysql;
import ctrip.framework.drc.mysql.EmbeddedDb;

import java.util.Map;

/**
 * @Author limingdong
 * @create 2021/4/7
 */
public class DbInitTask implements NamedCallable<EmbeddedMysql> {

    private int port;

    private String registryKey;

    public DbInitTask(int port, String registryKey) {
        this.port = port;
        this.registryKey = registryKey;
    }

    @Override
    public EmbeddedMysql call() {
        Map<String, Object> variables =  MySQLVariablesConfiguration.getInstance().getVariables(registryKey);
        return new EmbeddedDb().mysqlServer(port, variables);
    }
}
