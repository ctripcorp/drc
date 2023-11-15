package com.ctrip.framework.drc.core.monitor.operator;

import com.ctrip.framework.drc.core.monitor.execution.Execution;

import java.sql.SQLException;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2019-12-05
 * copy from integration-test written by mingdongli
 */
public interface WriteSqlOperator extends SqlOperator {

    /**
     * write 包含insert、update、delete
     * @param execution
     * @return
     */
    void write(Execution execution) throws SQLException;

    /**
     * write 包含insert、update、delete
     * @param execution
     * @return
     */
    StatementExecutorResult writeWithResult(Execution execution) throws SQLException;

    void insert(Execution execution) throws SQLException;

    void update(Execution execution) throws SQLException;

    void delete(Execution execution) throws SQLException;
}
