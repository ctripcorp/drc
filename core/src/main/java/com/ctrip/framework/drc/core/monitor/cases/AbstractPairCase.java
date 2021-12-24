package com.ctrip.framework.drc.core.monitor.cases;

import com.ctrip.framework.drc.core.monitor.operator.ReadSqlOperator;
import com.ctrip.framework.drc.core.monitor.alarm.Alarm;
import com.ctrip.framework.drc.core.monitor.alarm.LogAlarm;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by mingdongli
 * 2019/10/9 下午11:56.
 */
public abstract class AbstractPairCase<S extends ReadSqlOperator, D extends ReadSqlOperator> implements PairCase<S, D> {

    protected static Logger logger = LoggerFactory.getLogger(AbstractPairCase.class);

    protected Alarm alarm = new LogAlarm();

    protected List<PairCase> pairCases = Lists.newArrayList();

    @Override
    public void test(S source, D dst) {
        for (PairCase pairCase : pairCases) {
            pairCase.test(source, dst);
        }
    }
}
