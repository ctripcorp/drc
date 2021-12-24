package com.ctrip.framework.drc.monitor.function.cases.truncate;

import com.ctrip.framework.drc.monitor.function.operator.ReadWriteSqlOperator;

/**
 * @Author limingdong
 * @create 2020/6/16
 */
public interface TableTruncate {

    boolean truncateTable(ReadWriteSqlOperator sqlOperator, String tableName);
}
