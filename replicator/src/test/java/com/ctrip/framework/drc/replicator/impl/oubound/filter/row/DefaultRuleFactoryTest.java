package com.ctrip.framework.drc.replicator.impl.oubound.filter.row;

import com.ctrip.framework.drc.core.server.common.enums.RowFilterType;
import com.ctrip.framework.drc.core.server.common.filter.row.RowsFilterContext;
import com.ctrip.framework.drc.core.server.common.filter.row.RowsFilterRule;
import com.ctrip.framework.drc.core.server.common.filter.row.RuleFactory;
import com.ctrip.xpipe.codec.JsonCodec;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static com.ctrip.framework.drc.core.server.common.filter.row.RuleFactory.ROWS_FILTER_RULE;

/**
 * @Author limingdong
 * @create 2022/4/26
 */
public class DefaultRuleFactoryTest {

    private RuleFactory ruleFactory = new DefaultRuleFactory();

    private Map<String, List<String>> table2Id = Maps.newHashMap();

    private List<String> fields = Lists.newArrayList();

    private RowsFilterContext rowsFilterContext;

    @Test
    public void createRowsFilterRule() {
        fields.add("id");
        table2Id.put("drc1.insert1", fields);
        rowsFilterContext = RowsFilterContext.from(RowFilterType.Uid, JsonCodec.INSTANCE.encode(table2Id));
        RowsFilterRule rowsFilterRule = ruleFactory.createRowsFilterRule(rowsFilterContext);
        Assert.assertTrue(rowsFilterRule instanceof UidRowsFilterRule);

        rowsFilterContext = RowsFilterContext.from(RowFilterType.None, JsonCodec.INSTANCE.encode(table2Id));
        rowsFilterRule = ruleFactory.createRowsFilterRule(rowsFilterContext);
        Assert.assertTrue(rowsFilterRule instanceof NoopRowsFilterRule);

        System.setProperty(ROWS_FILTER_RULE, "com.ctrip.framework.drc.replicator.impl.oubound.filter.row.CustomRowsFilterRule");
        rowsFilterContext = RowsFilterContext.from(RowFilterType.Custom, JsonCodec.INSTANCE.encode(table2Id));
        rowsFilterRule = ruleFactory.createRowsFilterRule(rowsFilterContext);
        Assert.assertTrue(rowsFilterRule instanceof CustomRowsFilterRule);
    }
}