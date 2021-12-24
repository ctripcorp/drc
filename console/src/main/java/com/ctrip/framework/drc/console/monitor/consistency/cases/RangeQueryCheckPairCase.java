package com.ctrip.framework.drc.console.monitor.consistency.cases;

import com.ctrip.framework.drc.console.monitor.consistency.sql.execution.NameAwareExecution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by mingdongli
 * 2019/11/15 上午9:48.
 */
public class RangeQueryCheckPairCase extends ComparablePairCase {

    protected Logger logger = LoggerFactory.getLogger("consistencyMonitorLogger");

    public RangeQueryCheckPairCase(NameAwareExecution execution) {
        super(execution);
    }
}
