package com.ctrip.framework.drc.core.server.common.filter.row;

import com.ctrip.framework.drc.core.meta.RowsFilterConfig;
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
    protected List<List<Object>> doFilterRows(List<List<Object>> values, LinkedHashMap<String, Integer> indices) {
        Assert.assertEquals(3, values.size());
        Assert.assertEquals(2, indices.size());  // id、one
        Assert.assertEquals(0, indices.get("id").intValue());  // id in index 0
        Assert.assertEquals(1, indices.get("one").intValue());  // one in index 1
        return AbstractEventTest.result;
    }
}
