package com.ctrip.framework.drc.applier.resource.context.sql;

import java.sql.PreparedStatement;

/**
 * @Author Slight
 * Mar 16, 2020
 */
public interface PreparedStatementExecutor {

    PreparedStatementExecutor DEFAULT = new OneRowPreparedStatementExecutor();

    StatementExecutorResult execute(PreparedStatement statement);
}
