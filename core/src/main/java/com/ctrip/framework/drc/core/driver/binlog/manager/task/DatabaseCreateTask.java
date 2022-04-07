package com.ctrip.framework.drc.core.driver.binlog.manager.task;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.apache.tomcat.jdbc.pool.DataSource;

import java.util.List;

/**
 * @Author limingdong
 * @create 2022/4/7
 */
public class DatabaseCreateTask extends BatchTask {

    public static final String CREATE_DB = "CREATE DATABASE IF NOT EXISTS %s;";

    public DatabaseCreateTask(List<String> dbs, Endpoint inMemoryEndpoint, DataSource inMemoryDataSource) {
        super(inMemoryEndpoint, inMemoryDataSource);
        for (String db : dbs) {
            this.sqls.add(String.format(CREATE_DB, db));
        }
    }
}
