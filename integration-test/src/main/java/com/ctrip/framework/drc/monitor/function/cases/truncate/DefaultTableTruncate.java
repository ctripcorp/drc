package com.ctrip.framework.drc.monitor.function.cases.truncate;

import com.ctrip.framework.drc.core.monitor.execution.Execution;
import com.ctrip.framework.drc.monitor.function.cases.WriteCase;
import com.ctrip.framework.drc.monitor.function.cases.delete.TableTruncateCase;
import com.ctrip.framework.drc.monitor.function.execution.delete.SingleDeleteExecution;
import com.ctrip.framework.drc.monitor.function.operator.ReadWriteSqlOperator;

/**
 * @Author limingdong
 * @create 2020/6/16
 */
public class DefaultTableTruncate implements TableTruncate {

    @Override
    public boolean truncateTable(ReadWriteSqlOperator sqlOperator, String tableName) {
        Execution tranExecution = new SingleDeleteExecution(tableName);
        WriteCase writeCase = new TableTruncateCase(tranExecution);
        return writeCase.executeWrite(sqlOperator);
    }
}
