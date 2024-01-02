package com.ctrip.framework.drc.core.monitor.operator;

import java.sql.ResultSet;

/**
 * Created by dengquanliang
 * 2023/12/28 11:24
 */
public interface WriteResource extends AutoCloseable {
    ResultSet getResultSet();

    void close();
}
