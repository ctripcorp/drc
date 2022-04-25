package com.ctrip.framework.drc.monitor.replicator;

import com.ctrip.framework.drc.core.driver.binlog.impl.AbstractRowsEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.fetcher.event.MonitoredUpdateRowsEvent;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.row.RowsFilterResult;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.row.RowsFilterRule;
import com.ctrip.xpipe.api.codec.GenericTypeReference;
import com.ctrip.xpipe.codec.JsonCodec;
import org.assertj.core.util.Lists;

import java.util.List;
import java.util.Map;

/**
 * @Author limingdong
 * @create 2022/4/24
 */
public class EvenNumberRowsFilterRule implements RowsFilterRule<AbstractRowsEvent> {

    private static final int NOT_FOUND = -1;

    private Map<String, String> table2Id;

    public EvenNumberRowsFilterRule(String context) {
        this.table2Id = JsonCodec.INSTANCE.decode(context, new GenericTypeReference<>() {});
    }

    @Override
    public RowsFilterResult<AbstractRowsEvent> filterRow(AbstractRowsEvent rowsEvent, TableMapLogEvent tableMapLogEvent, TableMapLogEvent drcTableMapLogEvent) {
        String table = tableMapLogEvent.getSchemaNameDotTableName();
        if (table2Id == null || table2Id.get(table) == null) {
            return new RowsFilterResult(false);
        }

        String id = table2Id.get(table);

        rowsEvent.load(tableMapLogEvent.getColumns());
        List<List<Object>> values;
        if (rowsEvent instanceof MonitoredUpdateRowsEvent) {
            values = rowsEvent.getAfterPresentRowsValues();
        } else {
            values = rowsEvent.getBeforePresentRowsValues();
        }

        int index = NOT_FOUND;
        List<TableMapLogEvent.Column> columns = drcTableMapLogEvent.getColumns();
        for (int i = 0; i < columns.size(); ++i) {
            if (columns.get(i).getName().equalsIgnoreCase(id)) {
                index = i;
                break;
            }
        }

        if (index == NOT_FOUND) {
            return new RowsFilterResult(false);
        }

        List<AbstractRowsEvent.Row> rows = rowsEvent.getRows();
        List<Boolean> res = Lists.newArrayList();
        for (int i = 0; i < values.size(); ++i) {
            long idValue = (long) values.get(i).get(index);
            if (idValue % 2 == 0) {
                rows.add(rows.get(index));
            }
        }

        return new RowsFilterResult(true, res);
    }
}
