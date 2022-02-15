package com.ctrip.framework.drc.console.monitor.table.task;

import com.ctrip.framework.drc.console.monitor.DefaultCurrentMetaManager;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.pojo.MetaKey;
import com.ctrip.framework.drc.console.task.AbstractMasterMySQLEndpointObserver;
import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.monitor.entity.ConsistencyEntity;
import com.ctrip.framework.drc.core.monitor.enums.ConsistencyEnum;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultReporterHolder;
import com.ctrip.framework.drc.core.server.observer.endpoint.MasterMySQLEndpointObserver;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.ArrayUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.CONSOLE_TABLE_LOGGER;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-02-26
 * Periodically check the cluster's table structure consistency:
 * 1. among master DBs in all DCs
 * 2. between the one (1st columns info actually) applier reported to ZK and the one in the dbCluster which the applier belongs to
 */
@Order(2)
@Component
public class CheckTableConsistencyTask extends AbstractMasterMySQLEndpointObserver implements MasterMySQLEndpointObserver {

    @Autowired
    private DbClusterSourceProvider dbClusterSourceProvider;

    @Autowired
    private MonitorTableSourceProvider monitorTableSourceProvider;

    @Autowired
    private DefaultCurrentMetaManager currentMetaManager;

    private static final String SWITCH_STATUS_ON = "on";

    private Map<String, ConsistencyEntity> consistencyEntityMap = Maps.newConcurrentMap();

    private List<Set<DbClusterSourceProvider.Mha>> mhaGroups;

    private Map<String, Boolean> consistencyMapper = new ConcurrentHashMap<>();

    protected Map<String, Boolean> getConsistencyMapper() {
        return Collections.unmodifiableMap(consistencyMapper);
    }

    @Override
    public void initialize() {
        super.initialize();
        mhaGroups = dbClusterSourceProvider.getMhaGroups();
        currentMetaManager.addObserver(this);
    }

    @Override
    public void scheduledTask() {
            String tableConsistencyMonitorSwitch = monitorTableSourceProvider.getTableConsistencySwitch();
            if(SWITCH_STATUS_ON.equalsIgnoreCase(tableConsistencyMonitorSwitch)) {
                List<List<DbClusterSourceProvider.Mha>> mhaCombinationList = dbClusterSourceProvider.getAllMhaCombinationList();
                for(List<DbClusterSourceProvider.Mha> mhaCombination : mhaCombinationList) {
                    if(isFilteredOut(mhaCombination)) {
                        continue;
                    }
                    DbClusterSourceProvider.Mha srcMha = mhaCombination.get(0);
                    DbCluster srcDbCluster = srcMha.getDbCluster();
                    DbClusterSourceProvider.Mha destMha = mhaCombination.get(1);
                    DbCluster destDbCluster = destMha.getDbCluster();
                    ConsistencyEntity consistencyEntity = getConsistencyEntity(srcMha, destMha);
                    MetaKey srcMetaKey = new MetaKey(srcMha.getDc(), srcDbCluster.getId(), srcDbCluster.getName(), srcDbCluster.getMhaName());
                    MetaKey dstMetaKey = new MetaKey(destMha.getDc(), destDbCluster.getId(), destDbCluster.getName(), destDbCluster.getMhaName());
                    Endpoint srcEndpoint = masterMySQLEndpointMap.get(srcMetaKey);
                    Endpoint destEndpoint = masterMySQLEndpointMap.get(dstMetaKey);
                    boolean consistency = checkTableConsistency(srcEndpoint, destEndpoint, srcDbCluster.getMhaName(), destDbCluster.getMhaName(), srcDbCluster.getName());
                    if(consistency) {
                        CONSOLE_TABLE_LOGGER.info("[[monitor=tableConsistency,direction={}:{},cluster={}]][Report] Table is consistent between two DCs': {}:{} and {}:{}", srcDbCluster.getMhaName(), destDbCluster.getMhaName(), srcDbCluster.getName(), srcEndpoint.getHost(), srcEndpoint.getPort(), destEndpoint.getHost(), destEndpoint.getPort());
                        DefaultReporterHolder.getInstance().reportTableConsistency(consistencyEntity, ConsistencyEnum.CONSISTENT);
                    } else {
                        DefaultReporterHolder.getInstance().reportTableConsistency(consistencyEntity, ConsistencyEnum.NON_CONSISTENT);
                    }
                    consistencyMapper.put(srcDbCluster.getMhaName()+"."+destDbCluster.getMhaName(), consistency);
                }
            }
    }

    protected boolean checkTableConsistency(Endpoint srcEndpoint, Endpoint destEndpoint, String srcMha, String destMha, String cluster) {
        /**
         * table structure comparision: show create table statement comparison
         */
        // key: Columns, value: the number of occurrences for the Columns between two DCs
        Map<String, String> srcStmts = MySqlUtils.getDefaultCreateTblStmts(srcEndpoint);
        Map<String, String> destStmts = MySqlUtils.getDefaultCreateTblStmts(destEndpoint);
        String tableDiff = checkTableDiff(srcStmts, destStmts);
        if(null != tableDiff) {
            CONSOLE_TABLE_LOGGER.info("[[monitor=tableConsistency,direction={}:{},cluster={}]][Report] Something is wrong between two DCs' db: {}:{} and {}:{}. Check these tables: {}", srcMha, destMha, cluster, srcEndpoint.getHost(), srcEndpoint.getPort(), destEndpoint.getHost(), destEndpoint.getPort(), tableDiff);
            return false;
        }
        for(String table : srcStmts.keySet()) {
            String srcStmt = srcStmts.get(table);
            String destStmt = destStmts.get(table);
            if(!srcStmt.equalsIgnoreCase(destStmt)) {
                CONSOLE_TABLE_LOGGER.info("[[monitor=tableConsistency,direction={}:{},cluster={}]][Report] Table {} is different between two DCs' db: {}:{} and {}:{}", srcMha, destMha, cluster, table, srcEndpoint.getHost(), srcEndpoint.getPort(), destEndpoint.getHost(), destEndpoint.getPort());
                return false;
            }
        }
        return true;
    }

    private String checkTableDiff(Map<String, String> srcStmts, Map<String, String> destStmts) {
        Set<String> tableDiff = new HashSet<>(srcStmts.keySet());
        tableDiff.removeAll(destStmts.keySet());
        if(!tableDiff.isEmpty()) {
            return tableDiff.toString();
        }
        tableDiff.addAll(destStmts.keySet());
        tableDiff.removeAll(srcStmts.keySet());
        if(!tableDiff.isEmpty()) {
            return tableDiff.toString();
        }
        return null;
    }

    protected ConsistencyEntity getConsistencyEntity(DbClusterSourceProvider.Mha srcMha, DbClusterSourceProvider.Mha destMha) {
        DbCluster srcCluster = srcMha.getDbCluster();
        DbCluster destCluster = destMha.getDbCluster();
        String combinationKey = srcCluster.getMhaName() + "." + destCluster.getMhaName();
        if(consistencyEntityMap.containsKey(combinationKey)) {
            return consistencyEntityMap.get(combinationKey);
        }
        Endpoint srcEndpoint = dbClusterSourceProvider.getMaster(srcCluster);
        Endpoint destEndpoint = dbClusterSourceProvider.getMaster(destCluster);
        ConsistencyEntity consistencyEntity = new ConsistencyEntity.Builder()
                .clusterAppId(srcCluster.getAppId())
                .buName(srcCluster.getBuName())
                .srcDcName(srcMha.getDc())
                .clusterName(srcCluster.getName())
                .mhaName(srcCluster.getMhaName())
                .destDcName(destMha.getDc())
                .destMhaName(destCluster.getMhaName())
                .srcMysqlIp(srcEndpoint.getHost())
                .srcMysqlPort(srcEndpoint.getPort())
                .destMysqlIp(destEndpoint.getHost())
                .destMysqlPort(destEndpoint.getPort())
                .build();
        consistencyEntityMap.put(combinationKey, consistencyEntity);
        return consistencyEntity;
    }

    protected boolean isFilteredOut(List<DbClusterSourceProvider.Mha> mhas) {
        String[] filterOutMhasForMultiSide = monitorTableSourceProvider.getFilterOutMhasForMultiSideMonitor();
        for (DbClusterSourceProvider.Mha mha : mhas) {
            if(ArrayUtils.contains(filterOutMhasForMultiSide, mha.getDbCluster().getMhaName())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void setLocalDcName() {

    }

    @Override
    public void setOnlyCareLocal() {
        this.onlyCareLocal = false;
    }

    @Override
    public void clearOldEndpointResource(Endpoint endpoint) {
        MySqlUtils.removeSqlOperator(endpoint);
    }
}
