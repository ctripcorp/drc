package com.ctrip.framework.drc.replicator.impl.inbound.schema.task;

import com.wix.mysql.EmbeddedMysql;

/**
 * @Author limingdong
 * @create 2022/10/26
 */
public class MySQLInstanceCreator implements MySQLInstance {

    private EmbeddedMysql embeddedDb;

    public MySQLInstanceCreator(EmbeddedMysql embeddedDb) {
        this.embeddedDb = embeddedDb;
    }

    @Override
    public void destroy () {
        embeddedDb.destroy();
    }
}
