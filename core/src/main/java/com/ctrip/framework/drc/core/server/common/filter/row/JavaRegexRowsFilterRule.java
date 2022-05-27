package com.ctrip.framework.drc.core.server.common.filter.row;

import com.ctrip.framework.drc.core.driver.binlog.impl.AbstractRowsEvent;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Author limingdong
 * @create 2022/4/28
 */
public class JavaRegexRowsFilterRule extends AbstractRowsFilterRule implements RowsFilterRule<List<AbstractRowsEvent.Row>> {

    private Pattern pattern;

    public JavaRegexRowsFilterRule(RowsFilterConfig rowsFilterConfig) {
        super(rowsFilterConfig);
        pattern = Pattern.compile(context);
    }

    @Override
    protected boolean doFilterRows(Object field) throws Exception {
        return DefaultTransactionMonitorHolder.getInstance().logTransaction("DRC.replicator.rows.filter.regex", registryKey, () -> {
            Matcher matcher =  pattern.matcher(String.valueOf(field));
            return matcher.find();
        });
    }
}
