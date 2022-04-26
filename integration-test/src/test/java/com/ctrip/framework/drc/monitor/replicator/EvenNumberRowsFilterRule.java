package com.ctrip.framework.drc.monitor.replicator;

import com.ctrip.framework.drc.core.server.common.filter.row.AbstractRowsFilterRule;
import com.ctrip.framework.drc.core.server.common.filter.row.RowsFilterRule;
import com.google.common.collect.Lists;

import java.util.List;


/**
 * @Author limingdong
 * @create 2022/4/24
 */
public class EvenNumberRowsFilterRule extends AbstractRowsFilterRule implements RowsFilterRule<List<List<Object>> > {

    public EvenNumberRowsFilterRule(String context) {
        super(context);
    }

    @Override
    protected List<List<Object>> doFilterRows(List<List<Object>> values, List<Integer> indices) {
        List<List<Object>> res = Lists.newArrayList();
        for (int i = 0; i < values.size(); ++i) {
                int idValue = (int) values.get(i).get(indices.get(0));  // indices.size == 1
                if (idValue % 2 == 0) {
                    res.add(values.get(i));
                }
        }
        return res;
    }
}
