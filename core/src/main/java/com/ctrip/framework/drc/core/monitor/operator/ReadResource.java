package com.ctrip.framework.drc.core.monitor.operator;

import java.sql.ResultSet;

/**
 * Created by mingdongli
 * 2019/12/15 上午9:48.
 */
public interface ReadResource extends AutoCloseable {

    ResultSet getResultSet();

    void close();
}
