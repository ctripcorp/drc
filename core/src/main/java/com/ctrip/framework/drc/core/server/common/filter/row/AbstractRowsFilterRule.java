package com.ctrip.framework.drc.core.server.common.filter.row;

import com.ctrip.framework.drc.core.driver.binlog.impl.AbstractRowsEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.driver.schema.data.Columns;
import com.ctrip.xpipe.api.codec.GenericTypeReference;
import com.ctrip.xpipe.codec.JsonCodec;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.update_rows_event_v2;

/**
 * @Author limingdong
 * @create 2022/4/26
 */
public abstract class AbstractRowsFilterRule implements RowsFilterRule<List<List<Object>> > {

    protected Map<String, List<String>> table2Fields; // table -> multi field

    public AbstractRowsFilterRule(String context) {
        this.table2Fields = JsonCodec.INSTANCE.decode(context, new GenericTypeReference<>() {});
    }

    @Override
    public RowsFilterResult<List<List<Object>> > filterRow(AbstractRowsEvent rowsEvent, TableMapLogEvent drcTableMapLogEvent) {
        String table = drcTableMapLogEvent.getSchemaNameDotTableName();
        if (table2Fields == null || table2Fields.get(table) == null) {
            return new RowsFilterResult(true);
        }
        List<String> fields = table2Fields.get(table);

        Columns columns = Columns.from(drcTableMapLogEvent.getColumns());
        List<List<Object>> values = loadValues(rowsEvent);

        List<Integer> indices = indices(columns, fields);
        if (indices == null) {
            return new RowsFilterResult(true);
        }

        List<List<Object>> filteredRow = doRowsFilter(values, indices);

        return new RowsFilterResult(false, filteredRow);
    }

    protected List<List<Object>> loadValues(AbstractRowsEvent rowsEvent) {
        List<List<Object>> values;
        if (update_rows_event_v2 == rowsEvent.getLogEventType()) {
            values = rowsEvent.getAfterPresentRowsValues();
        } else {
            values = rowsEvent.getBeforePresentRowsValues();
        }

        return values;
    }

    private List<Integer> indices(Columns columns, List<String> fields) {
        List<Integer> indices = Lists.newArrayList(fields.size());

        int found = 0;
        for (int i = 0; i < indices.size(); ++i) {
            for (int j = 0; j < columns.size(); ++j) {
                if (columns.get(j).getName().equalsIgnoreCase(fields.get(i))) {
                    indices.set(i, j);
                    found++;
                    break;
                }
            }
        }

        return found == fields.size() ? indices : null;
    }

    protected abstract List<List<Object>> doRowsFilter(List<List<Object>> values, List<Integer> indices);
}
