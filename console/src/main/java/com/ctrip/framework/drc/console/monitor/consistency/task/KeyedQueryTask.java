package com.ctrip.framework.drc.console.monitor.consistency.task;

import com.ctrip.framework.drc.console.monitor.consistency.cases.KeyedQueryCheckPairCase;
import com.ctrip.framework.drc.console.monitor.consistency.cases.Row;
import com.ctrip.framework.drc.console.monitor.consistency.sql.execution.KeyedQuerySingleExecution;
import com.ctrip.framework.drc.console.monitor.consistency.table.TableNameDescriptor;

import java.util.Map;
import java.util.Set;

/**
 * Created by mingdongli
 * 2019/11/15 下午4:41.
 */
public class KeyedQueryTask extends AbstractQueryTask implements QueryTask<Map<String, Row>> {

    public KeyedQueryTask(TableNameDescriptor tableDescriptor, Set<String> keys) {
        awareExecution = new KeyedQuerySingleExecution(tableDescriptor, keys);
        comparablePairCase = new KeyedQueryCheckPairCase(awareExecution);
    }

}
