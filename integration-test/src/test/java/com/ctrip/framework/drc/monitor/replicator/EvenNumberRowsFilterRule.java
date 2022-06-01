package com.ctrip.framework.drc.monitor.replicator;

import com.ctrip.framework.drc.core.driver.binlog.impl.AbstractRowsEvent;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;
import com.ctrip.framework.drc.core.server.common.filter.row.AbstractRowsFilterRule;
import com.ctrip.framework.drc.core.server.common.filter.row.RowsFilterContext;
import com.ctrip.framework.drc.core.server.common.filter.row.RowsFilterRule;
import com.google.common.collect.Lists;

import java.util.LinkedHashMap;
import java.util.List;


/**
 * @Author limingdong
 * @create 2022/4/24
 */
public class EvenNumberRowsFilterRule extends AbstractRowsFilterRule implements RowsFilterRule<List<AbstractRowsEvent.Row> > {

    public static final String ID = "id";

    public EvenNumberRowsFilterRule(RowsFilterConfig rowsFilterConfig) {
        super(rowsFilterConfig);
    }

    @Override
    protected List<AbstractRowsEvent.Row> doFilterRows(AbstractRowsEvent rowsEvent, RowsFilterContext rowFilterContext, LinkedHashMap<String, Integer> indices) {
        List<AbstractRowsEvent.Row> result = Lists.newArrayList();
        List<List<Object>> values = getValues(rowsEvent);
        List<AbstractRowsEvent.Row> rows = rowsEvent.getRows();
        for (int i = 0; i < values.size(); ++i) {
            int idValue = (int) values.get(i).get(indices.get(ID));  // indices.size == 1
            if (idValue % 2 == 0) {
                result.add(rows.get(i));
            }
        }
        return result;
    }
}
