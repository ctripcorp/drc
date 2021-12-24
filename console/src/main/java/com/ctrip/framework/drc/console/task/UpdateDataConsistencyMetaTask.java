package com.ctrip.framework.drc.console.task;

import com.ctrip.framework.drc.console.dao.entity.DataConsistencyMonitorTbl;
import com.ctrip.framework.drc.console.dao.entity.MhaTbl;
import com.ctrip.framework.drc.console.monitor.DefaultCurrentMetaManager;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.config.DelayMonitorConfig;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.pojo.MetaKey;
import com.ctrip.framework.drc.console.service.impl.MetaInfoServiceImpl;
import com.ctrip.framework.drc.console.service.impl.MetaGenerator;
import com.ctrip.framework.drc.console.service.monitor.impl.ConsistencyConsistencyMonitorServiceImpl;
import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.MySqlEndpoint;
import com.ctrip.framework.drc.core.server.observer.endpoint.SlaveMySQLEndpointObserver;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider.SWITCH_STATUS_ON;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.CONSOLE_DC_LOGGER;

/**
 * @Author: hbshen
 * @Date: 2021/4/27
 */
@Order(2)
@Component
public class UpdateDataConsistencyMetaTask extends AbstractSlaveMySQLEndpointObserver implements SlaveMySQLEndpointObserver {

    @Autowired
    private DbClusterSourceProvider sourceProvider;

    @Autowired
    private MonitorTableSourceProvider monitorTableSourceProvider;

    @Autowired
    private DefaultCurrentMetaManager currentMetaManager;

    @Autowired
    private MetaInfoServiceImpl metaInfoService;

    @Autowired
    private ConsistencyConsistencyMonitorServiceImpl monitorService;

    @Autowired
    private MetaGenerator metaService;

    public static final int INITIAL_DELAY = 30;

    public static final int PERIOD = 120;

    public static final TimeUnit TIME_UNIT = TimeUnit.SECONDS;

    private static final int DRC_MHA_SIZE = 2;

    @Override
    public void initialize() {
        super.initialize();
        currentMetaManager.addObserver(this);
    }

    @Override
    public void scheduledTask() {
        String updateConsistencySwitch = monitorTableSourceProvider.getUpdateConsistencyMetaSwitch();
        if(SWITCH_STATUS_ON.equalsIgnoreCase(updateConsistencySwitch)) {
            updateConsistencyMeta();
        }
    }

    protected void updateConsistencyMeta() {
        for (Map.Entry<MetaKey, MySqlEndpoint> entry : slaveMySQLEndpointMap.entrySet()) {
            MetaKey metaKey = entry.getKey();
            MySqlEndpoint mySqlEndpoint = entry.getValue();
            String mhaName = metaKey.getMhaName();
            CONSOLE_DC_LOGGER.debug("update consistency meta for {}({})", metaKey, mySqlEndpoint.getSocketAddress());
            try {
                Long mhaGroupId = metaInfoService.getMhaGroupId(mhaName);
                if(null != mhaGroupId) {
                    List<MhaTbl> mhaTbls = metaInfoService.getMhaTbls(mhaGroupId);
                    if(null != mhaTbls && mhaTbls.size() >= DRC_MHA_SIZE) {
                        Map<String, DelayMonitorConfig> delayMonitorConfigs = MySqlUtils.getDefaultDelayMonitorConfigs(mySqlEndpoint);
                        addAndDeleteConsistencyMeta(mhaTbls, delayMonitorConfigs);
                    }
                }
            } catch (Throwable t) {
                CONSOLE_DC_LOGGER.warn("Fail to update consistency meta, will try next round", t);
                MySqlUtils.removeSqlOperator(mySqlEndpoint);
            }
        }
    }

    protected Pair<Integer, Integer> addAndDeleteConsistencyMeta(List<MhaTbl> mhaTblsInSameGroup, Map<String, DelayMonitorConfig> curDelayMonitorConfigs) throws SQLException {
        int added = 0, deleted = 0;
        Set<Long> mhaIds = mhaTblsInSameGroup.stream().map(MhaTbl::getId).collect(Collectors.toSet());
        List<Integer> idsTobeDeleted = getIdsToBeDeleted(mhaIds, curDelayMonitorConfigs);
        List<DelayMonitorConfig> delayMonitorConfigsToBeAdded = getDelayMonitorConfigsToBeAdded(mhaIds, curDelayMonitorConfigs);
        String mhaA = mhaTblsInSameGroup.get(0).getMhaName();
        String mhaB = mhaTblsInSameGroup.get(1).getMhaName();
        for(DelayMonitorConfig delayMonitorConfig : delayMonitorConfigsToBeAdded) {
            try {
                CONSOLE_DC_LOGGER.info("[[monitor=dataConsistency]]add table({}:{}:{}) for mhas({}:{})", delayMonitorConfig.getTable(), delayMonitorConfig.getKey(), delayMonitorConfig.getOnUpdate(), mhaA, mhaB);
                monitorService.addDataConsistencyMonitor(mhaA, mhaB, delayMonitorConfig);
                added++;
            } catch (SQLException e) {
                CONSOLE_DC_LOGGER.warn("[[monitor=dataConsistency]]Fail add table({}:{}:{}) for mhas({}:{})", delayMonitorConfig.getTable(), delayMonitorConfig.getKey(), delayMonitorConfig.getOnUpdate(), mhaA, mhaB, e);
            }
        }
        for(int idToBeDeleted : idsTobeDeleted) {
            try {
                CONSOLE_DC_LOGGER.info("[[monitor=dataConsistency]]deleted table id({}) for mhas({}:{})", idsTobeDeleted, mhaA, mhaB);
                monitorService.deleteDataConsistencyMonitor(idToBeDeleted);
                deleted++;
            } catch (SQLException e) {
                CONSOLE_DC_LOGGER.warn("[[monitor=dataConsistency]]Fail delete table id({}) for mhas({}:{})", idsTobeDeleted, mhaA, mhaB, e);
            }
        }
        return new Pair<>(added, deleted);
    }

    protected List<Integer> getIdsToBeDeleted(Set<Long> mhaIdsInSameGroup, Map<String, DelayMonitorConfig> delayMonitorConfigs) throws SQLException {
        Set<String> tableSchemasInDb = Sets.newHashSet();
        delayMonitorConfigs.keySet().forEach(tableChema -> {
            tableSchemasInDb.add(tableChema.replaceAll("`", "").replaceAll(" ", "").toLowerCase());
        });
        return metaService.getDataConsistencyMonitorTbls().stream()
                .filter(p -> (mhaIdsInSameGroup.contains((long) p.getMhaId()) && !tableSchemasInDb.contains(p.getMonitorSchemaName() + "." + p.getMonitorTableName())))
                .map(DataConsistencyMonitorTbl::getId)
                .collect(Collectors.toList());
    }

    protected List<DelayMonitorConfig> getDelayMonitorConfigsToBeAdded(Set<Long> mhaIdsInSameGroup, Map<String, DelayMonitorConfig> delayMonitorConfigs) throws SQLException {
        Set<String> tableSchemasConfigured = Sets.newHashSet();
        List<DelayMonitorConfig> delayMonitorConfigsToBeAdded = Lists.newArrayList();
        metaService.getDataConsistencyMonitorTbls().stream().filter(p -> mhaIdsInSameGroup.contains((long) p.getMhaId())).forEach(dataConsistencyMonitorTbl -> {
            tableSchemasConfigured.add(String.format("`%s`.`%s`", dataConsistencyMonitorTbl.getMonitorSchemaName(), dataConsistencyMonitorTbl.getMonitorTableName()));
        });
        for(Map.Entry<String, DelayMonitorConfig> entry : delayMonitorConfigs.entrySet()) {
            if(!tableSchemasConfigured.contains(entry.getKey())) {
                delayMonitorConfigsToBeAdded.add(entry.getValue());
            }
        }
        return delayMonitorConfigsToBeAdded;
    }

    @Override
    public void clearOldEndpointResource(Endpoint endpoint) {
        MySqlUtils.removeSqlOperator(endpoint);
    }

    @Override
    public void setLocalDcName() {
        this.localDcName = sourceProvider.getLocalDcName();
    }

    @Override
    public void setOnlyCareLocal() {
        this.onlyCareLocal = true;
    }

    @Override
    public int getDefaultInitialDelay() {
        return INITIAL_DELAY;
    }

    @Override
    public int getDefaultPeriod(){
        return PERIOD;
    }

    @Override
    public TimeUnit getDefaultTimeUnit() {
        return TIME_UNIT;
    }
}
