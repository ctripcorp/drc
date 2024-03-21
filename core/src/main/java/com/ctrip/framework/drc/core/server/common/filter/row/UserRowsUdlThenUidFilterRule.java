package com.ctrip.framework.drc.core.server.common.filter.row;

import com.ctrip.framework.drc.core.driver.binlog.impl.AbstractRowsEvent;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.framework.drc.core.monitor.util.ServicesUtil;
import com.ctrip.framework.drc.core.server.common.filter.service.UserService;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * @Author limingdong
 * @create 2022/4/22
 */
public class UserRowsUdlThenUidFilterRule extends UserRowsFilterRule implements RowsFilterRule<List<AbstractRowsEvent.Row>> {

    private UserService uidService = ServicesUtil.getUidService();

    public UserRowsUdlThenUidFilterRule(RowsFilterConfig rowsFilterConfig) {
        super(rowsFilterConfig);
    }

    @Override
    protected RowsFilterResult.Status doFilterRows(Object field, RowsFilterConfig.Parameters parameters) throws Exception {
        String filterMode = parameters.getUserFilterMode();
        UserFilterMode userFilterMode = UserFilterMode.getUserFilterMode(filterMode);
        if (UserFilterMode.Udl == userFilterMode) {
            return DefaultTransactionMonitorHolder.getInstance().logTransaction("DRC.replicator.rows.filter.udl", registryKey, () -> {
                if (this.isFieldEmpty(field)) {
                    return RowsFilterResult.Status.Illegal;
                }
                UserContext userContext = fetchUserContext(field, parameters);
                userContext.setDrcStrategyId(drcStrategyId);
                return uidService.filterUdl(userContext);
            });
        } else if (UserFilterMode.Uid == userFilterMode) {
            int fetchMode = parameters.getFetchMode();
            if (FetchMode.RPC.getCode() == fetchMode) {
                return DefaultTransactionMonitorHolder.getInstance().logTransaction("DRC.replicator.rows.filter.uid", registryKey, () -> uidService.filterUid(fetchUserContext(field, parameters)));
            }
            throw new UnsupportedOperationException("not support for fetchMode " + fetchMode);
        }
        throw new UnsupportedOperationException("not support for filterMode " + filterMode);
    }

    private boolean isFieldEmpty(Object field) {
        return field == null || (field instanceof String && StringUtils.isEmpty((String) field));
    }
}
