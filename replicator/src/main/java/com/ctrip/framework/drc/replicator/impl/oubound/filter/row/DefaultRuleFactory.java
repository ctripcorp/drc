package com.ctrip.framework.drc.replicator.impl.oubound.filter.row;

import com.ctrip.framework.drc.core.server.common.enums.RowFilterType;
import com.ctrip.framework.drc.core.server.common.filter.row.RowsFilterContext;
import com.ctrip.framework.drc.core.server.common.filter.row.RowsFilterRule;
import com.ctrip.framework.drc.core.server.common.filter.row.RuleFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;

/**
 * @Author limingdong
 * @create 2022/4/22
 */
public class DefaultRuleFactory implements RuleFactory {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public RowsFilterRule createRowsFilterRule(RowsFilterContext context) {
        RowFilterType rowFilterType = context.getFilterType();
        String filterContext = context.getFilterContext();
        try {
            Class<? extends RowsFilterRule> rowsFilterRule;
            if (RowFilterType.Uid == rowFilterType) {
                rowsFilterRule = UidRowsFilterRule.class;
            } else if (RowFilterType.IT == rowFilterType) {
                String clazz = System.getProperty(ROWS_FILTER_RULE);
                rowsFilterRule = (Class<RowsFilterRule>) Class.forName(clazz);
            } else {
                rowsFilterRule = NoopRowsFilterRule.class;
            }

            Constructor constructor = rowsFilterRule.getConstructor(new Class[]{String.class});
            return (RowsFilterRule) constructor.newInstance(filterContext);

        } catch (Exception e) {
            logger.error("[RowsFilterRule] init error", e);
        }

        return new NoopRowsFilterRule(filterContext);
    }
}
