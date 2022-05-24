package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.driver.binlog.impl.AbstractRowsEvent;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;
import com.ctrip.framework.drc.core.server.common.filter.row.AbstractRowsFilterRule;
import com.ctrip.framework.drc.core.server.common.filter.row.RowsFilterContext;
import com.google.common.collect.Lists;
import org.junit.Assert;

import java.util.LinkedHashMap;
import java.util.List;

/**
 * @Author limingdong
 * @create 2022/4/26
 */
public class CustomRowsFilterRule extends AbstractRowsFilterRule {

    public CustomRowsFilterRule(RowsFilterConfig rowsFilterConfig) {
        super(rowsFilterConfig);
    }

    @Override
    protected List<AbstractRowsEvent.Row> doFilterRows(AbstractRowsEvent rowsEvent, LinkedHashMap<String, Integer> indices, RowsFilterContext rowFilterContext) {
        Assert.assertEquals(3, rowsEvent.getRows().size());
        Assert.assertEquals(2, indices.size());  // id、one
        Assert.assertEquals(0, indices.get("id").intValue());  // id in index 0
        Assert.assertEquals(1, indices.get("one").intValue());  // one in index 1

        List<AbstractRowsEvent.Row> res = Lists.newArrayList();
        res.add(rowsEvent.getRows().get(0));
        res.add(rowsEvent.getRows().get(2));
        return res;
    }
}
