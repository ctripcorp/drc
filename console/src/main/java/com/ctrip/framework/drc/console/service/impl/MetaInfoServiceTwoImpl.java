package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.entity.DataConsistencyMonitorTbl;
import com.ctrip.framework.drc.console.dao.entity.MhaTbl;
import com.ctrip.framework.drc.console.dao.entity.ProxyTbl;
import com.ctrip.framework.drc.console.dao.entity.ResourceTbl;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.TableEnum;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.pojo.MetaKey;
import com.ctrip.framework.drc.console.pojo.MonitorMetaInfo;
import com.ctrip.framework.drc.console.service.monitor.MonitorService;
import com.ctrip.framework.drc.console.utils.DalUtils;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.MySqlEndpoint;
import com.ctrip.framework.drc.core.entity.*;
import com.ctrip.framework.drc.core.monitor.enums.ModuleEnum;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class MetaInfoServiceTwoImpl {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private DbClusterSourceProvider sourceProvider;

    @Autowired
    private MetaInfoServiceImpl metaInfoService;

    @Autowired
    private MonitorService monitorService;
    
    @Autowired
    private DefaultConsoleConfig consoleConfig;

    private DalUtils dalUtils = DalUtils.getInstance();

    Map<MetaKey, MySqlEndpoint> masterMySQLEndpoint;

    Map<MetaKey, MySqlEndpoint> slaveMySQLEndpoint;

    Map<MetaKey, Endpoint> masterReplicatorEndpoint;

    private MonitorMetaInfo monitorMetaInfo;

    public List<String> getResources(String dc, String type) {
        try {
            Integer typeCode = ModuleEnum.getModuleEnum(type).getCode();
            Long dcId = dalUtils.getId(TableEnum.DC_TABLE, dc);
            List<String> resourceIps = dalUtils.getResourceTblDao().queryAll().stream()
                    .filter(p -> p.getDeleted().equals(BooleanEnum.FALSE.getCode()) &&
                            p.getType().equals(typeCode) &&
                            p.getDcId().equals(dcId))
                    .map(ResourceTbl::getIp)
                    .collect(Collectors.toList());
            return resourceIps;
        } catch (Exception e) {
            logger.error("Fail get resources for {} in {}", type, dc);
            return Lists.newArrayList();
        }
    }

    public MonitorMetaInfo getMonitorMetaInfo() throws SQLException {
        refreshMetaEndpoints();
        return monitorMetaInfo;
    }

    private void refreshMetaEndpoints() throws SQLException {
        List<String> mhaNamesToBeMonitored = monitorService.getMhaNamesToBeMonitored();
        monitorMetaInfo = new MonitorMetaInfo();
        masterMySQLEndpoint = Maps.newConcurrentMap();
        slaveMySQLEndpoint = Maps.newConcurrentMap();
        masterReplicatorEndpoint = Maps.newConcurrentMap();


        try {
            Drc drc = sourceProvider.getDrc();
            if(drc == null) {
                logger.info("[MetaInfoServiceTwoImpl] return drc null");
                return;
            }
            for(Dc dc : drc.getDcs().values()) {
                for(DbCluster dbCluster : dc.getDbClusters().values()) {
                    String mhaName = dbCluster.getMhaName();
                    if(!mhaNamesToBeMonitored.contains(mhaName)) {
                        continue;
                    }
                    MetaKey metaKey = new MetaKey.Builder()
                            .dc(dc.getId())
                            .clusterId(dbCluster.getId())
                            .clusterName(dbCluster.getName())
                            .mhaName(dbCluster.getMhaName())
                            .build();

                    Replicator masterReplicator = dbCluster.getReplicators().stream()
                            .filter(Replicator::isMaster).findFirst()
                            .orElse(dbCluster.getReplicators().stream().findFirst().orElse(null));
                    Dbs dbs = dbCluster.getDbs();
                    String monitorUser = dbs.getMonitorUser();
                    String monitorPassword = dbs.getMonitorPassword();
                    List<Db> dbList = dbs.getDbs();
                    Db masterDb = dbList.stream()
                            .filter(Db::isMaster).findFirst().orElse(null);
                    Db slaveDb = dbList.stream()
                            .filter(db -> !db.isMaster()).findFirst().orElse(null);

                    if(masterReplicator != null) {
                        masterReplicatorEndpoint.put(metaKey, new DefaultEndPoint(masterReplicator.getIp(), masterReplicator.getApplierPort()));
                        logger.info("[META] one masterReplicatorEndpoint mhaName is {},ipPort is {}:{}",metaKey.getMhaName(),masterReplicator.getIp(), masterReplicator.getApplierPort());
                    } else {
                        logger.warn("[NO META] no master replicator for: {}", metaKey);
                    }
                    if(masterDb != null) {
                        masterMySQLEndpoint.put(metaKey, new MySqlEndpoint(masterDb.getIp(), masterDb.getPort(), monitorUser, monitorPassword, true));
                        logger.info("[META] one masterMySQLEndpoint mhaName is {},ipPort is {}:{}",metaKey.getMhaName(),masterDb.getIp(), masterDb.getPort());
                    } else {
                        logger.warn("[NO META] no master mysql for: {}", metaKey);
                    }
                    if(slaveDb != null) {
                        slaveMySQLEndpoint.put(metaKey, new MySqlEndpoint(slaveDb.getIp(), slaveDb.getPort(), monitorUser, monitorPassword, false));
                        logger.info("[META] one slaveMySQLEndpoint mhaName is {},ipPort is {}:{}",metaKey.getMhaName(),slaveDb.getIp(), slaveDb.getPort());
                    } else {
                        logger.warn("[NO META] no slave mysql for: {}", metaKey);
                    }
                }
            }
            monitorMetaInfo.setMasterMySQLEndpoint(masterMySQLEndpoint);
            monitorMetaInfo.setSlaveMySQLEndpoint(slaveMySQLEndpoint);
            monitorMetaInfo.setMasterReplicatorEndpoint(masterReplicatorEndpoint);
        } catch (Exception e) {
            logger.error("Fail get master replicator endpoint, ", e);
        }
    }

    public List<DataConsistencyMonitorTbl> getAllDataConsistencyMonitorTbl(String mhaName) throws SQLException {
        Long mhaGroupId = metaInfoService.getMhaGroupId(mhaName);
        List<Long> mhaTblIds = metaInfoService.getMhaTbls(mhaGroupId).stream().map(MhaTbl::getId).collect(Collectors.toList());
        return dalUtils.getDataConsistencyMonitorTblDao().queryAll().stream()
                .filter(p -> p.getMonitorSwitch().equals(BooleanEnum.TRUE.getCode()) && mhaTblIds.contains((long) p.getMhaId()))
                .collect(Collectors.toList());
    }

    public List<String> getProxyUris(String dc) throws Throwable {
        Set<String> dcsInSameRegion = consoleConfig.getDcsInSameRegion(dc);
        List<String> proxyUris = Lists.newArrayList();
        for (String InSameRegion : dcsInSameRegion) {
            Long dcId = dalUtils.getId(TableEnum.DC_TABLE, InSameRegion);
            dalUtils.getProxyTblDao().queryByDcId(dcId,BooleanEnum.FALSE.getCode()).forEach(proxyTbl -> proxyUris.add(proxyTbl.getUri()));
        } 
        return proxyUris;
    }

    public List<String> getAllProxyUris() throws Throwable {
        return dalUtils.getProxyTblDao().queryAll().stream().filter(p -> p.getDeleted().equals(BooleanEnum.FALSE.getCode())).map(ProxyTbl::getUri).collect(Collectors.toList());
    }

}
