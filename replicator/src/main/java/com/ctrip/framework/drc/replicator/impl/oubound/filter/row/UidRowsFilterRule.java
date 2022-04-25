package com.ctrip.framework.drc.replicator.impl.oubound.filter.row;

import com.ctrip.framework.drc.core.driver.binlog.impl.AbstractRowsEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.xpipe.api.codec.GenericTypeReference;
import com.ctrip.xpipe.codec.JsonCodec;

import java.util.Map;

/**
 * @Author limingdong
 * @create 2022/4/22
 */
public class UidRowsFilterRule implements RowsFilterRule<AbstractRowsEvent> {

    private Map<String, String> table2Uid;

    public UidRowsFilterRule(String context) {
        this.table2Uid = JsonCodec.INSTANCE.decode(context, new GenericTypeReference<>() {});
    }

    @Override
    public RowsFilterResult<AbstractRowsEvent> filterRow(AbstractRowsEvent rowsEvent, TableMapLogEvent tableMapLogEvent, TableMapLogEvent drcTableMapLogEvent) {
        // TODO
        return new RowsFilterResult(false);

    }
}
