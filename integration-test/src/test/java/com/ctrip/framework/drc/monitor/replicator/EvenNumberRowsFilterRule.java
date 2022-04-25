package com.ctrip.framework.drc.monitor.replicator;

import com.ctrip.framework.drc.core.driver.binlog.impl.AbstractRowsEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.driver.schema.data.Columns;
import com.ctrip.framework.drc.fetcher.event.MonitoredUpdateRowsEvent;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.row.RowsFilterResult;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.row.RowsFilterRule;
import com.ctrip.xpipe.api.codec.GenericTypeReference;
import com.ctrip.xpipe.codec.JsonCodec;
import org.assertj.core.util.Lists;

import java.util.List;
import java.util.Map;

import static com.ctrip.framework.drc.fetcher.event.FetcherRowsEvent.transformMetaAndType;

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
            return new RowsFilterResult(true);
        }

        String id = table2Id.get(table);

        Columns originColumns = Columns.from(tableMapLogEvent.getColumns());
        Columns columns = Columns.from(drcTableMapLogEvent.getColumns());
        transformMetaAndType(originColumns, columns);
        rowsEvent.load(columns);

        List<List<Object>> values;
        if (rowsEvent instanceof MonitoredUpdateRowsEvent) {
            values = rowsEvent.getAfterPresentRowsValues();
        } else {
            values = rowsEvent.getBeforePresentRowsValues();
        }

        int index = NOT_FOUND;
        for (int i = 0; i < columns.size(); ++i) {
            if (columns.get(i).getName().equalsIgnoreCase(id)) {
                index = i;
                break;
            }
        }

        if (index == NOT_FOUND) {
            return new RowsFilterResult(true);
        }

        List<AbstractRowsEvent.Row> rows = rowsEvent.getRows();
        List<Boolean> res = Lists.newArrayList();
        for (int i = 0; i < values.size(); ++i) {
            int idValue = (int) values.get(i).get(index);
            if (idValue % 2 == 0) {
                rows.add(rows.get(index));
            }
        }

        // build new event
        return new RowsFilterResult(false, rowsEvent);
    }
}
