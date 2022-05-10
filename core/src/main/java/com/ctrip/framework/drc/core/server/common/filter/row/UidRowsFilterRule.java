package com.ctrip.framework.drc.core.server.common.filter.row;

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
public class UidRowsFilterRule extends AbstractRowsFilterRule implements RowsFilterRule<List<List<Object>>> {

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
    protected List<List<Object>> doFilterRows(List<List<Object>> values, LinkedHashMap<String, Integer> indices) throws Exception {
        List<List<Object>> result = Lists.newArrayList();
        int index = indices.values().iterator().next(); // only one field
        for (List<Object> row : values) {
            Object uid = row.get(index);
            DefaultTransactionMonitorHolder.getInstance().logTransaction("DRC.replicator.rows.filter.sdk", registryKey, () -> {
                if (uidService.filterUid(String.valueOf(uid), dstLocation)) {
                    result.add(row);
                }
            });

        }
        return result;
    }
}
