package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.meta.RowsFilterConfig;
import com.ctrip.framework.drc.core.server.common.filter.row.AbstractRowsFilterRule;
import com.google.common.collect.Lists;
import org.junit.Assert;

import java.util.List;
import java.util.Map;

/**
 * @Author limingdong
 * @create 2022/4/26
 */
public class CustomRowsFilterRule extends AbstractRowsFilterRule {

    public CustomRowsFilterRule(RowsFilterConfig rowsFilterConfig) {
        super(rowsFilterConfig);
    }

    @Override
    protected List<List<Object>> doFilterRows(List<List<Object>> values, Map<String, Integer> indices) {
        Assert.assertEquals(3, values.size());
        Assert.assertEquals(2, indices.size());  // id、one
        Assert.assertEquals(0, indices.get("id").intValue());  // id in index 0
        Assert.assertEquals(1, indices.get("one").intValue());  // one in index 1

        List<List<Object>> res = Lists.newArrayList();
        List<Object> data = Lists.newArrayList(1,2);
        res.add(data);
        return res;
    }
}
