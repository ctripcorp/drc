package com.ctrip.framework.drc.core.driver.binlog.gtid.db;

import java.sql.Connection;

/**
 * Created by jixinwang on 2021/9/15
 */
public interface GtidReader {
    String getExecutedGtids(Connection connection);
}
