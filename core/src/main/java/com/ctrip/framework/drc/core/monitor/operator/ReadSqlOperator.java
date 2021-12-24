package com.ctrip.framework.drc.core.monitor.operator;

import com.ctrip.framework.drc.core.monitor.execution.Execution;

import java.sql.SQLException;

/**
 * Created by mingdongli
 * 2019/10/9 下午5:38.
 */
public interface ReadSqlOperator<R> extends SqlOperator {

    R select(Execution execution) throws SQLException;
}
