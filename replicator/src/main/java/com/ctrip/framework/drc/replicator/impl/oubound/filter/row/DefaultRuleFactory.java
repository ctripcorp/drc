package com.ctrip.framework.drc.replicator.impl.oubound.filter.row;

import com.ctrip.framework.drc.core.server.common.enums.RowFilterType;
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
        try {
            RowFilterType rowFilterType = context.getFilterType();
            Class<? extends RowsFilterRule> rowsFilterRule;
            if (RowFilterType.Uid == rowFilterType) {
                rowsFilterRule = UidRowsFilterRule.class;
//            Map<String, String> table2Uid = JsonCodec.INSTANCE.decode((String) context.getFilterContext(), new GenericTypeReference<>() {});
//            return new UidRowsFilterRule(table2Uid);
            } else if (RowFilterType.IT == rowFilterType) {
                String clazz = System.getProperty(ROWS_FILTER_RULE);
                rowsFilterRule = (Class<RowsFilterRule>) Class.forName(clazz);
            } else {
                rowsFilterRule = NoopRowsFilterRule.class;
            }

            Constructor constructor = rowsFilterRule.getConstructor(new Class[]{String.class});
            return (RowsFilterRule) constructor.newInstance((String) context.getFilterContext());

        } catch (Exception e) {
            logger.error("[RowsFilterRule] init error", e);
        }

        return new NoopRowsFilterRule();
    }
}
