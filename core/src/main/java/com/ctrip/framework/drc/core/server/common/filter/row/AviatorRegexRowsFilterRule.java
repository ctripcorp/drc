package com.ctrip.framework.drc.core.server.common.filter.row;

import com.ctrip.framework.drc.core.driver.binlog.impl.AbstractRowsEvent;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;
import com.google.common.collect.Lists;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author limingdong
 * @create 2022/4/28
 */
public class AviatorRegexRowsFilterRule extends AbstractRowsFilterRule implements RowsFilterRule<List<AbstractRowsEvent.Row>> {

    private Expression expression;

    public AviatorRegexRowsFilterRule(RowsFilterConfig rowsFilterConfig) {
        super(rowsFilterConfig);
        expression = AviatorEvaluator.compile(context, true);
    }

    protected List<AbstractRowsEvent.Row> doFilterRows(AbstractRowsEvent rowsEvent, RowsFilterContext rowFilterContext, LinkedHashMap<String, Integer> indices) throws Exception {
        List<AbstractRowsEvent.Row> result = Lists.newArrayList();
        List<List<Object>> values = getValues(rowsEvent);
        List<AbstractRowsEvent.Row> rows = rowsEvent.getRows();
        int arraySize = indices.size() * 2;
        Object[] array = new Object[arraySize];
        for (int i = 0; i < values.size(); ++i) {
            int j = 0;
            for (Map.Entry<String, Integer> entry : indices.entrySet()) {
                array[2 * j] = entry.getKey();
                array[2 * j + 1] = values.get(i).get(entry.getValue());
                j++;
            }
            ArrayWrapper wrapper = new ArrayWrapper(array);
            Boolean cache = rowFilterContext.get(wrapper);
            if (cache == null) {
                cache = (boolean)expression.execute(expression.newEnv(array));
                rowFilterContext.putIfAbsent(wrapper, cache);
            }
            if (cache) {
                result.add(rows.get(i));
            }
        }
        return result;
    }

    public static class ArrayWrapper {
        private Object[] array;

        public ArrayWrapper(Object[] array) {
            this.array = array;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof ArrayWrapper)) return false;
            ArrayWrapper that = (ArrayWrapper) o;
            return Arrays.equals(array, that.array);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(array);
        }
    }
}
