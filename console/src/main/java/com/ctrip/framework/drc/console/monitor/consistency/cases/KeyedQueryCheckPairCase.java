package com.ctrip.framework.drc.console.monitor.consistency.cases;

import com.ctrip.framework.drc.console.monitor.consistency.sql.execution.NameAwareExecution;

/**
 * Created by mingdongli
 * 2019/11/15 下午4:12.
 */
public class KeyedQueryCheckPairCase extends ComparablePairCase {

    public KeyedQueryCheckPairCase(NameAwareExecution execution) {
        super(execution);
    }
}
