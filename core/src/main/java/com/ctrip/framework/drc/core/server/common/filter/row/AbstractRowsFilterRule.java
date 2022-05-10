package com.ctrip.framework.drc.core.server.common.filter.row;

import com.ctrip.framework.drc.core.driver.binlog.impl.AbstractRowsEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.driver.schema.data.Columns;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;
import com.google.common.collect.Maps;

import java.util.LinkedHashMap;
import java.util.List;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.update_rows_event_v2;

/**
 * @Author limingdong
 * @create 2022/4/26
 */
public abstract class AbstractRowsFilterRule implements RowsFilterRule<List<List<Object>> > {

    protected String registryKey;

    protected List<String> fields;

    protected String context;

    public AbstractRowsFilterRule(RowsFilterConfig rowsFilterConfig) {
        this.registryKey = rowsFilterConfig.getRegistryKey();
        RowsFilterConfig.Parameters parameters = rowsFilterConfig.getParameters();
        if (parameters != null) {
            this.context = parameters.getContext();
            this.fields = parameters.getColumns();
        }
    }

    @Override
    public RowsFilterResult<List<List<Object>>> filterRows(AbstractRowsEvent rowsEvent, TableMapLogEvent drcTableMapLogEvent) throws Exception {
        Columns columns = Columns.from(drcTableMapLogEvent.getColumns());
        List<List<Object>> values = getValues(rowsEvent);

        LinkedHashMap<String, Integer> indices = getIndices(columns, fields);
        if (indices == null) {
            return new RowsFilterResult(true);
        }

        List<List<Object>> filteredRow = doFilterRows(values, indices);

        if (filteredRow != null && values.size() == filteredRow.size()) {
            return new RowsFilterResult(true);
        }
        return new RowsFilterResult(false, filteredRow);
    }

    protected List<List<Object>> getValues(AbstractRowsEvent rowsEvent) {
        List<List<Object>> values;
        if (update_rows_event_v2 == rowsEvent.getLogEventType()) {
            values = rowsEvent.getAfterPresentRowsValues();
        } else {
            values = rowsEvent.getBeforePresentRowsValues();
        }

        return values;
    }

    protected LinkedHashMap<String, Integer> getIndices(Columns columns, List<String> fields) {
        final int fieldSize = fields.size();
        LinkedHashMap<String, Integer> integerMap = Maps.newLinkedHashMap();

        int found = 0;
        for (int i = 0; i < fieldSize; ++i) {
            for (int j = 0; j < columns.size(); ++j) {
                String colName = columns.get(j).getName();
                if (colName.equalsIgnoreCase(fields.get(i))) {
                    integerMap.put(colName, j);
                    found++;
                    break;
                }
            }
        }

        return found == fieldSize ? integerMap : null;
    }

    /**
     *
     * @param values column values in binlog
     * @param indices column name to its index in values
     * @return
     * @throws Exception
     */
    protected abstract List<List<Object>> doFilterRows(List<List<Object>> values, LinkedHashMap<String, Integer> indices) throws Exception;
}
