package com.ctrip.framework.drc.core.driver.binlog.manager.task;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.apache.tomcat.jdbc.pool.DataSource;

import java.util.List;

/**
 * @Author limingdong
 * @create 2022/4/7
 */
public class DatabaseDropTask extends BatchTask {

    public static final String DROP_DATABASE = "DROP DATABASE IF EXISTS %s";

    public DatabaseDropTask(List<String> dbs, Endpoint inMemoryEndpoint, DataSource inMemoryDataSource) {
        super(inMemoryEndpoint, inMemoryDataSource);
        for (String db : dbs) {
            this.sqls.add(String.format(DROP_DATABASE, db));
        }
    }
}
