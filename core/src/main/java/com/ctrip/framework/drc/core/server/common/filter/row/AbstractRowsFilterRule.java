package com.ctrip.framework.drc.core.server.common.filter.row;

import com.ctrip.framework.drc.core.driver.binlog.impl.AbstractRowsEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.driver.schema.data.Columns;
import com.ctrip.xpipe.api.codec.GenericTypeReference;
import com.ctrip.xpipe.codec.JsonCodec;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.update_rows_event_v2;

/**
 * @Author limingdong
 * @create 2022/4/26
 */
public abstract class AbstractRowsFilterRule implements RowsFilterRule<List<List<Object>> > {

    protected String registryKey;

    protected Map<String, List<String>> table2Fields; // table -> multi fields

    public AbstractRowsFilterRule(String keyName, String context) {
        this.registryKey = keyName;
        this.table2Fields = JsonCodec.INSTANCE.decode(context, new GenericTypeReference<>() {});
    }

    @Override
    public RowsFilterResult<List<List<Object>>> filterRows(AbstractRowsEvent rowsEvent, TableMapLogEvent drcTableMapLogEvent) {
        String table = drcTableMapLogEvent.getSchemaNameDotTableName();
        if (table2Fields == null || table2Fields.get(table) == null) {
            return new RowsFilterResult(true);
        }
        List<String> fields = table2Fields.get(table);

        Columns columns = Columns.from(drcTableMapLogEvent.getColumns());
        List<List<Object>> values = getValues(rowsEvent);

        Map<String, Integer> indices = getIndices(columns, fields);
        if (indices == null) {
            return new RowsFilterResult(true);
        }

        List<List<Object>> filteredRow = doFilterRows(values, indices);

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

    protected Map<String, Integer> getIndices(Columns columns, List<String> fields) {
        final int fieldSize = fields.size();
        Map<String, Integer> integerMap = Maps.newHashMap();

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

    protected abstract List<List<Object>> doFilterRows(List<List<Object>> values, Map<String, Integer> indices);
}
