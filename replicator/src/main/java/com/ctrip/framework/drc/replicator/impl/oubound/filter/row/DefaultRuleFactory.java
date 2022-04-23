package com.ctrip.framework.drc.replicator.impl.oubound.filter.row;

import com.ctrip.framework.drc.core.server.common.enums.RowFilterType;
import com.ctrip.xpipe.api.codec.GenericTypeReference;
import com.ctrip.xpipe.codec.JsonCodec;

import java.util.Map;

/**
 * @Author limingdong
 * @create 2022/4/22
 */
public class DefaultRuleFactory implements RuleFactory {

    @Override
    public RowsFilterRule createRowsFilterRule(RowsFilterContext context) {
        if (RowFilterType.Uid == context.getFilterType()) {
            Map<String, String> table2Uid = JsonCodec.INSTANCE.decode((String) context.getFilterContext(), new GenericTypeReference<>() {});
            return new UidRowsFilterRule(table2Uid);
        }
        return new NoopRowsFilterRule();
    }
}
