package com.ctrip.framework.drc.core.server.common.filter.row;

import com.ctrip.framework.drc.core.driver.binlog.impl.AbstractRowsEvent;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.framework.drc.core.monitor.util.ServicesUtil;
import com.ctrip.framework.drc.core.server.common.filter.service.UserService;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.COMMA;

/**
 * @Author limingdong
 * @create 2022/4/22
 */
public class UserRowsFilterRule extends AbstractRowsFilterRule implements RowsFilterRule<List<AbstractRowsEvent.Row>> {

    private UserService uidService = ServicesUtil.getUidService();

    private UidConfiguration uidConfiguration = UidConfiguration.getInstance();

    private UidGlobalConfiguration uidGlobalConfiguration = UidGlobalConfiguration.getInstance();

    private Set<String> dstLocation = Sets.newHashSet();

    protected int drcStrategyId;

    public UserRowsFilterRule(RowsFilterConfig rowsFilterConfig) {
        super(rowsFilterConfig);
        Collections.sort(parametersList);
        if (parametersList != null && !parametersList.isEmpty() && StringUtils.isNotBlank(parametersList.get(0).getContext())) {
            String[] locations = parametersList.get(0).getContext().split(COMMA);
            for (String l : locations) {
                dstLocation.add(Region.nameFor(l.trim().toUpperCase()).name());
            }
        }
        drcStrategyId = rowsFilterConfig.getConfigs() != null ? rowsFilterConfig.getConfigs().getDrcStrategyId() : -1;
    }

    @Override
    protected RowsFilterResult.Status doFilterRows(Object field, RowsFilterConfig.Parameters parameters) throws Exception {
        String filterMode = parameters.getUserFilterMode();
        UserFilterMode userFilterMode = UserFilterMode.getUserFilterMode(filterMode);
        if (UserFilterMode.Udl == userFilterMode) {
            return DefaultTransactionMonitorHolder.getInstance().logTransaction("DRC.replicator.rows.filter.udl", registryKey, () -> {
                UserContext userContext = fetchUserContext(field, parameters);
                userContext.setDrcStrategyId(drcStrategyId);
                return uidService.filterUdl(userContext);
            });
        } else if (UserFilterMode.Uid == userFilterMode) {
            int fetchMode = parameters.getFetchMode();
            if (FetchMode.RPC.getCode() == fetchMode) {
                return DefaultTransactionMonitorHolder.getInstance().logTransaction("DRC.replicator.rows.filter.uid", registryKey, () -> uidService.filterUid(fetchUserContext(field, parameters)));
            } else if (FetchMode.BlackList.getCode() == fetchMode) {
                return DefaultTransactionMonitorHolder.getInstance().logTransaction("DRC.replicator.rows.filter.blackList", registryKey, () -> RowsFilterResult.Status.from(uidConfiguration.filterRowsWithBlackList(fetchUserContext(field, parameters))));
            } else if (FetchMode.WhiteList.getCode() == fetchMode) {
                return DefaultTransactionMonitorHolder.getInstance().logTransaction("DRC.replicator.rows.filter.whiteList", registryKey, () -> RowsFilterResult.Status.from(uidConfiguration.filterRowsWithWhiteList(fetchUserContext(field, parameters))));
            } else if (FetchMode.BlackList_Global.getCode() == fetchMode) {
                return DefaultTransactionMonitorHolder.getInstance().logTransaction("DRC.replicator.rows.filter.blackList.global", registryKey, () -> RowsFilterResult.Status.from(uidGlobalConfiguration.filterRowsWithBlackList(fetchUserContext(field, parameters))));
            }

            throw new UnsupportedOperationException("not support for fetchMode " + fetchMode);
        }

        throw new UnsupportedOperationException("not support for filterMode " + filterMode);
    }

    protected UserContext fetchUserContext(Object field, RowsFilterConfig.Parameters parameters) {
        UserContext uidContext = new UserContext();
        uidContext.setUserAttr(String.valueOf(field));
        uidContext.setLocations(dstLocation);
        uidContext.setIllegalArgument(parameters.getIllegalArgument());
        uidContext.setRegistryKey(registryKey);

        return uidContext;
    }
}
