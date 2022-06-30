package com.ctrip.framework.drc.core.server.common.filter.row;

import com.ctrip.framework.drc.core.driver.binlog.impl.AbstractRowsEvent;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.framework.drc.core.monitor.util.ServicesUtil;
import com.ctrip.framework.drc.core.server.common.filter.service.UidService;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Set;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.COMMA;

/**
 * @Author limingdong
 * @create 2022/4/22
 */
public class UidRowsFilterRule extends AbstractRowsFilterRule implements RowsFilterRule<List<AbstractRowsEvent.Row>> {

    private UidService uidService = ServicesUtil.getUidService();

    private UidConfiguration uidConfiguration = UidConfiguration.getInstance();

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
    protected boolean doFilterRows(Object field) throws Exception {
        if (FetchMode.RPC == fetchMode) {
            return DefaultTransactionMonitorHolder.getInstance().logTransaction("DRC.replicator.rows.filter.sdk", registryKey, () -> {
                if (uidService.filterUid(fetchUidContext(field))) {
                    return true;
                }
                return false;
            });
        } else if (FetchMode.BlackList == fetchMode) {
            return uidConfiguration.filterRowsWithBlackList(String.valueOf(field), registryKey);
        } else if (FetchMode.WhiteList == fetchMode) {
            return uidConfiguration.filterRowsWithWhiteList(String.valueOf(field), registryKey);
        }

        throw new UnsupportedOperationException("not support for fetchMode " + fetchMode);
    }

    private UidContext fetchUidContext(Object field) {
        UidContext uidContext = new UidContext();
        uidContext.setUid(String.valueOf(field));
        uidContext.setLocations(dstLocation);
        uidContext.setIllegalArgument(illegalArgument);

        return uidContext;
    }
}
