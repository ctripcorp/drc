package com.ctrip.framework.drc.monitor.function.cases;

import com.ctrip.framework.drc.core.monitor.execution.Execution;
import com.ctrip.framework.drc.core.monitor.operator.ReadResource;
import com.ctrip.framework.drc.core.monitor.operator.ReadSqlOperator;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by mingdongli
 * 2019/10/13 上午8:56.
 */
public class AbstractReadCase extends AbstractCase implements SelectCase {

    public AbstractReadCase(Execution execution) {
        super(execution);
    }

    @Override
    public ReadResource executeSelect(ReadSqlOperator<ReadResource> readSqlOperator) {
        try {
            return readSqlOperator.select(execution);
        } catch (SQLException e) {
            logger.error("[{}] execute select error {}", getClass().getSimpleName(), execution);
        }
        return null;
    }
}
