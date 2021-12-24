package com.ctrip.framework.drc.applier.resource.context.sql;

import com.mysql.jdbc.exceptions.jdbc4.MySQLIntegrityConstraintViolationException;
import com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException;

import java.sql.PreparedStatement;

import static com.ctrip.framework.drc.applier.resource.context.sql.StatementExecutorResult.TYPE.*;

/**
 * @Author Slight
 * Apr 21, 2020
 */
public class OneRowPreparedStatementExecutor implements PreparedStatementExecutor {

    @Override
    public StatementExecutorResult execute(PreparedStatement statement) {
        try {
            statement.execute();
            if (statement.getUpdateCount() == 1) {
                return StatementExecutorResult.of(UPDATE_COUNT_EQUALS_ONE);
            }
            if (statement.getUpdateCount() == 0) {
                return StatementExecutorResult.of(UPDATE_COUNT_EQUALS_ZERO);
            }
            return StatementExecutorResult.of(ERROR).with(statement.getUpdateCount() + " rows updated, UNLIKELY.");
        } catch (MySQLSyntaxErrorException e) {
            if (e.getMessage().startsWith("Unknown column")) {
                return StatementExecutorResult.of(UNKNOWN_COLUMN).with(e);
            } else {
                return StatementExecutorResult.of(ERROR).with(e);
            }
        } catch (MySQLIntegrityConstraintViolationException e) {
            if (e.getMessage().startsWith("Duplicate entry")) {
                return StatementExecutorResult.of(DUPLICATE_ENTRY).with(e);
            } else {
                return StatementExecutorResult.of(ERROR).with(e);
            }
        } catch (Throwable e) {
            if (e.getMessage().startsWith("No value specified for parameter")) {
                return StatementExecutorResult.of(NO_PARAMETER).with(e);
            }
            return StatementExecutorResult.of(ERROR).with(e);
        }
    }
}
