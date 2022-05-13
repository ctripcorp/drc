package com.ctrip.framework.drc.core.server.common.filter.row;

import com.ctrip.framework.drc.core.driver.binlog.impl.AbstractRowsEvent;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.framework.drc.core.monitor.util.ServicesUtil;
import com.ctrip.framework.drc.core.server.common.filter.service.UidService;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.COMMA;

/**
 * @Author limingdong
 * @create 2022/4/22
 */
public class UidRowsFilterRule extends AbstractRowsFilterRule implements RowsFilterRule<List<AbstractRowsEvent.Row>> {

    private UidService uidService = ServicesUtil.getUidService();

    private Set<String> dstLocation = Sets.newHashSet();

    public UidRowsFilterRule(RowsFilterConfig rowsFilterConfig) {
        super(rowsFilterConfig);
        if (StringUtils.isNotBlank(context)) {
            String[] locations = context.split(COMMA);
            for (String l : locations) {
                dstLocation.add(l.trim().toUpperCase());
            }
        }
    }

    @Override
    protected List<AbstractRowsEvent.Row> doFilterRows(AbstractRowsEvent rowsEvent, LinkedHashMap<String, Integer> indices) throws Exception {
        List<AbstractRowsEvent.Row> result = Lists.newArrayList();
        List<List<Object>> values = getValues(rowsEvent);
        List<AbstractRowsEvent.Row> rows = rowsEvent.getRows();
        int index = indices.values().iterator().next(); // only one field
        for (int i = 0; i < values.size(); ++i) {
            Object uid = values.get(i).get(index);
            int finalI = i;
            DefaultTransactionMonitorHolder.getInstance().logTransaction("DRC.replicator.rows.filter.sdk", registryKey, () -> {
                if (uidService.filterUid(String.valueOf(uid), dstLocation)) {
                    result.add(rows.get(finalI));
                }
            });

        }
        return result;
    }
}
