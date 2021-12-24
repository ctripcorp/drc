package com.ctrip.framework.drc.applier.resource.context.sql;

import com.ctrip.framework.drc.fetcher.event.transaction.TransactionData;
import com.ctrip.framework.drc.applier.resource.context.Batchable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import static com.ctrip.framework.drc.applier.resource.context.sql.StatementExecutorResult.TYPE.BATCHED;
import static com.ctrip.framework.drc.applier.resource.context.sql.StatementExecutorResult.TYPE.ERROR;

/**
 * @Author Slight
 * Apr 21, 2020
 */
public class BatchPreparedStatementExecutor implements PreparedStatementExecutor, Batchable {

    private static final Logger logger = LoggerFactory.getLogger(BatchPreparedStatementExecutor.class);

    private Statement statement;

    public BatchPreparedStatementExecutor(Statement statement) {
        this.statement = statement;
    }

    @Override
    public StatementExecutorResult execute(PreparedStatement statement) {
        try {
            this.statement.addBatch(statementToString(statement));
            return StatementExecutorResult.of(BATCHED);
        } catch (Throwable t) {
            return StatementExecutorResult.of(ERROR).with(t);
        }
    }

    private String statementToString(PreparedStatement statement) throws SQLException {
        return ((com.mysql.jdbc.PreparedStatement)statement).asSql();
    }

    @Override
    public TransactionData.ApplyResult executeBatch() {
        TransactionData.ApplyResult result = TransactionData.ApplyResult.SUCCESS;
        try {
            int[] rs = statement.executeBatch();
            for (int i = 0; i < rs.length; i++) {
                if (rs[i] == 0) {
                    result = TransactionData.ApplyResult.BATCH_ERROR;
                    break;
                }
            }
        } catch (Throwable t) {
            logger.error("executeBatch error", t);
            result = TransactionData.ApplyResult.BATCH_ERROR;
        }

        return result;
    }

    @Override
    public void dispose() {
        try {
            if (statement != null) {
                statement.close();
            }
        } catch (SQLException e) {
            logger.error("dispose error", e);
        }
    }
}
