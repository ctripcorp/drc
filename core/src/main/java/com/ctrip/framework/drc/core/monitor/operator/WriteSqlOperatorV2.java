package com.ctrip.framework.drc.core.monitor.operator;

import com.ctrip.framework.drc.core.monitor.execution.Execution;

import java.sql.SQLException;

/**
 * Created by dengquanliang
 * 2023/12/28 11:42
 */
public interface WriteSqlOperatorV2 extends SqlOperator {
    /**
     * write 包含insert、update、delete
     * @param execution
     * @return
     */
    StatementExecutorResult writeWithResult(Execution execution) throws SQLException;
}
