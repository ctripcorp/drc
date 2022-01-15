package com.ctrip.framework.drc.console.utils;

import com.ctrip.framework.drc.console.dao.*;
import com.ctrip.framework.drc.console.dao.entity.*;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.EstablishStatusEnum;
import com.ctrip.framework.drc.console.enums.SourceTypeEnum;
import com.ctrip.framework.drc.console.enums.TableEnum;
import com.ctrip.framework.drc.core.monitor.enums.ModuleEnum;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.DalQueryDao;
import com.ctrip.platform.dal.dao.KeyHolder;
import com.ctrip.platform.dal.dao.StatementParameters;
import com.ctrip.platform.dal.dao.sqlbuilder.FreeSelectSqlBuilder;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Map;

import static com.ctrip.framework.drc.console.common.bean.DalConfig.DRC_TITAN_KEY;
import static com.ctrip.framework.drc.console.config.ConsoleConfig.ZERO_ROWS_AFFECT;
import static com.ctrip.framework.drc.console.service.impl.AccessServiceImpl.*;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-09-14
 */
public class DalUtils {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private DcTblDao dcTblDao;
    private ResourceTblDao resourceTblDao;
    private ZookeeperTblDao zookeeperTblDao;
    private ClusterManagerTblDao clusterManagerTblDao;
    private BuTblDao buTblDao;
    private ClusterTblDao clusterTblDao;
    private MhaTblDao mhaTblDao;
    private MhaGroupTblDao mhaGroupTblDao;
    private RouteTblDao routeTblDao;
    private ProxyTblDao proxyTblDao;
    private ClusterMhaMapTblDao clusterMhaMapTblDao;
    private GroupMappingTblDao groupMappingTblDao;
    private MachineTblDao machineTblDao;
    private ReplicatorGroupTblDao replicatorGroupTblDao;
    private ReplicatorTblDao replicatorTblDao;
    private ApplierGroupTblDao applierGroupTblDao;
    private ApplierTblDao applierTblDao;
    private DdlHistoryTblDao ddlHistoryTblDao;
    private DataConsistencyMonitorTblDao dataConsistencyMonitorTblDao;
    private DataInconsistencyHistoryTblDao dataInconsistencyHistoryTblDao;
    private UnitRouteVerificationHistoryTblDao unitRouteVerificationHistoryTblDao;
    private DalQueryDao dalQueryDao;

    private static class DalUtilsHolder {
        public static final DalUtils INSTANCE = new DalUtils();
    }

    public static DalUtils getInstance() {
        return DalUtilsHolder.INSTANCE;
    }

    private DalUtils() {
        try {
            initDaos();
        } catch(SQLException e) {
            logger.error("Fail to init daos, ", e);
        }
    }

    private void initDaos() throws SQLException {
        dcTblDao = new DcTblDao();
        resourceTblDao = new ResourceTblDao();
        zookeeperTblDao = new ZookeeperTblDao();
        clusterManagerTblDao = new ClusterManagerTblDao();
        groupMappingTblDao = new GroupMappingTblDao();
        buTblDao = new BuTblDao();
        clusterTblDao = new ClusterTblDao();
        mhaTblDao = new MhaTblDao();
        mhaGroupTblDao = new MhaGroupTblDao();
        routeTblDao = new RouteTblDao();
        proxyTblDao = new ProxyTblDao();
        clusterMhaMapTblDao = new ClusterMhaMapTblDao();
        machineTblDao = new MachineTblDao();
        replicatorGroupTblDao = new ReplicatorGroupTblDao();
        replicatorTblDao = new ReplicatorTblDao();
        applierGroupTblDao = new ApplierGroupTblDao();
        applierTblDao = new ApplierTblDao();
        ddlHistoryTblDao = new DdlHistoryTblDao();
        dataConsistencyMonitorTblDao = new DataConsistencyMonitorTblDao();
        dataInconsistencyHistoryTblDao = new DataInconsistencyHistoryTblDao();
        unitRouteVerificationHistoryTblDao = new UnitRouteVerificationHistoryTblDao();
        dalQueryDao = new DalQueryDao(DRC_TITAN_KEY);
    }

    public Long getId(TableEnum tableEnum, String value) throws SQLException {
        FreeSelectSqlBuilder<Long> builder = new FreeSelectSqlBuilder<>();
        builder.setTemplate(tableEnum.selectById());
        StatementParameters parameters = new StatementParameters();
        int i = 1;
        parameters.set(i, tableEnum.getUniqueKeyType(), value);
        builder.simpleType().requireSingle().nullable();
        return dalQueryDao.query(builder, parameters, new DalHints());
    }

    public String getDcNameByDcId(Long dcId) throws SQLException {
        return dcTblDao.queryByPk(dcId).getDcName();
    }

    public Long insertBu(String buName) throws SQLException {
        BuTbl pojo = createBuPojo(buName);
        KeyHolder keyHolder = new KeyHolder();
        buTblDao.insert(new DalHints(), keyHolder, pojo);
        return (Long) keyHolder.getKey();
    }

    public Long updateOrCreateBu(String buName) throws SQLException {
        BuTbl buTbl = buTblDao.queryAll().stream().filter(p -> p.getBuName().equalsIgnoreCase(buName)).findFirst().orElse(null);
        if(null == buTbl) {
            return insertBu(buName);
        } else if (BooleanEnum.TRUE.getCode().equals(buTbl.getDeleted())) {
            buTbl.setDeleted(BooleanEnum.FALSE.getCode());
            buTblDao.update(buTbl);
        }
        return buTbl.getId();
    }

    public Long insertClusterMhaMap(Long clusterId, Long mhaId) throws SQLException {
        ClusterMhaMapTbl pojo = createClusterMhaMapPojo(clusterId, mhaId);
        KeyHolder keyHolder = new KeyHolder();
        clusterMhaMapTblDao.insert(new DalHints(), keyHolder, pojo);
        return (Long) keyHolder.getKey();
    }

    public Long updateOrCreateClusterMhaMap(Long clusterId, Long mhaId) throws SQLException {
        ClusterMhaMapTbl clusterMhaMapTbl = clusterMhaMapTblDao.queryAll().stream().filter(p -> (clusterId.equals(p.getClusterId()) && mhaId.equals(p.getMhaId()))).findFirst().orElse(null);
        if(null == clusterMhaMapTbl) {
            return insertClusterMhaMap(clusterId, mhaId);
        } else if (BooleanEnum.TRUE.getCode().equals(clusterMhaMapTbl.getDeleted())) {
            clusterMhaMapTbl.setDeleted(BooleanEnum.FALSE.getCode());
            clusterMhaMapTblDao.update(clusterMhaMapTbl);
        }
        return clusterMhaMapTbl.getId();
    }

    public Long insertGroupMapping(Long mhaGroupId, Long mhaId) throws SQLException {
        GroupMappingTbl pojo = new GroupMappingTbl();
        pojo.setMhaGroupId(mhaGroupId);
        pojo.setMhaId(mhaId);
        KeyHolder keyHolder = new KeyHolder();
        groupMappingTblDao.insert(new DalHints(), keyHolder, pojo);
        return (Long) keyHolder.getKey();
    }

    public Long updateOrCreateGroupMapping(Long mhaGroupId, Long mhaId) throws SQLException {
        GroupMappingTbl groupMappingTbl = groupMappingTblDao.queryAll().stream().filter(p -> (mhaGroupId.equals(p.getMhaGroupId()) && mhaId.equals(p.getMhaId()))).findFirst().orElse(null);
        if (null == groupMappingTbl) {
            return insertGroupMapping(mhaGroupId, mhaId);
        } else if (BooleanEnum.TRUE.getCode().equals(groupMappingTbl.getDeleted())) {
            groupMappingTbl.setDeleted(BooleanEnum.FALSE.getCode());
            groupMappingTblDao.update(groupMappingTbl);
        }
        return groupMappingTbl.getId();
    }

    public Long insertDc(String dcName) throws SQLException {
        DcTbl pojo = createDcPojo(dcName);
        KeyHolder keyHolder = new KeyHolder();
        dcTblDao.insert(new DalHints(), keyHolder, pojo);
        return (Long) keyHolder.getKey();
    }

    public Long updateOrCreateDc(String dc) throws SQLException {
        DcTbl dcTbl = dcTblDao.queryAll().stream().filter(p -> p.getDcName().equalsIgnoreCase(dc)).findFirst().orElse(null);
        if (null == dcTbl) {
            return insertDc(dc);
        } else if (BooleanEnum.TRUE.getCode().equals(dcTbl.getDeleted())){
            dcTbl.setDeleted(BooleanEnum.FALSE.getCode());
            dcTblDao.update(dcTbl);
        }
        return dcTbl.getId();
    }

    public Long insertCluster(String clusterName, Long clusterAppId, Long buId) throws SQLException {
        ClusterTbl pojo = createClusterPojo(clusterName, clusterAppId, buId);
        KeyHolder keyHolder = new KeyHolder();
        clusterTblDao.insert(new DalHints(), keyHolder, pojo);
        return (Long) keyHolder.getKey();
    }

    public Long updateOrCreateCluster(String clusterName, Long clusterAppId, Long buId) throws SQLException {
        ClusterTbl clusterTbl = clusterTblDao.queryAll().stream().filter(p -> p.getClusterName().equalsIgnoreCase(clusterName)).findFirst().orElse(null);
        if(null == clusterTbl) {
            return insertCluster(clusterName, clusterAppId, buId);
        } else if (!clusterAppId.equals(clusterTbl.getClusterAppId()) || !buId.equals(clusterTbl.getBuId()) || BooleanEnum.TRUE.getCode().equals(clusterTbl.getDeleted())) {
            clusterTbl.setClusterAppId(clusterAppId);
            clusterTbl.setBuId(buId);
            clusterTbl.setDeleted(BooleanEnum.FALSE.getCode());
            clusterTblDao.update(clusterTbl);
        }
        return clusterTbl.getId();
    }

    public Long insertMhaGroup(BooleanEnum drcStatus, EstablishStatusEnum establishStatusEnum, String readUser, String readPassword, String writeUser, String writePassword, String monitorUser, String monitorPassword) throws SQLException {
        MhaGroupTbl pojo = createMhaGroupPojo(drcStatus, establishStatusEnum, readUser, readPassword, writeUser, writePassword, monitorUser, monitorPassword);
        KeyHolder keyHolder = new KeyHolder();
        mhaGroupTblDao.insert(new DalHints(), keyHolder, pojo);
        return (Long) keyHolder.getKey();
    }

    public int updateMhaGroup(MhaGroupTbl mhaGroupTbl) throws SQLException {
        return mhaGroupTblDao.update(mhaGroupTbl);
    }

    public Long updateOrCreateMhaGroup(String mhaName, BooleanEnum drcStatus, EstablishStatusEnum establishStatusEnum, Map<String, String> usersAndPasswords) throws SQLException {
        MhaTbl mhaTbl = mhaTblDao.queryAll().stream().filter(p -> p.getMhaName().equalsIgnoreCase(mhaName)).findFirst().orElse(null);
        if(null == mhaTbl || null == mhaTbl.getMhaGroupId()) {
            return insertMhaGroup(drcStatus, establishStatusEnum, usersAndPasswords.get(READ_USER_KEY), usersAndPasswords.get(READ_PASSWORD_KEY), usersAndPasswords.get(WRITE_USER_KEY), usersAndPasswords.get(WRITE_PASSWORD_KEY), usersAndPasswords.get(MONITOR_USER_KEY), usersAndPasswords.get(MONITOR_PASSWORD_KEY));
        } else {
            MhaGroupTbl mhaGroupTbl = mhaGroupTblDao.queryByPk(mhaTbl.getMhaGroupId());
            mhaGroupTbl.setDrcStatus(drcStatus.getCode());
            mhaGroupTbl.setDrcEstablishStatus(establishStatusEnum.getCode());
            mhaGroupTbl.setReadUser(usersAndPasswords.get(READ_USER_KEY));
            mhaGroupTbl.setReadPassword(usersAndPasswords.get(READ_PASSWORD_KEY));
            mhaGroupTbl.setWriteUser(usersAndPasswords.get(WRITE_USER_KEY));
            mhaGroupTbl.setWritePassword(usersAndPasswords.get(WRITE_PASSWORD_KEY));
            mhaGroupTbl.setMonitorUser(usersAndPasswords.get(MONITOR_USER_KEY));
            mhaGroupTbl.setMonitorPassword(usersAndPasswords.get(MONITOR_PASSWORD_KEY));
            mhaGroupTbl.setDeleted(BooleanEnum.FALSE.getCode());
            mhaGroupTblDao.update(mhaGroupTbl);
        }
        return mhaTbl.getMhaGroupId();
    }

    public Long insertMha(String mhaName, Long mhaGroupId, Long dcId) throws SQLException {
        MhaTbl pojo = createMhaPojo(mhaName, mhaGroupId, dcId);
        KeyHolder keyHolder = new KeyHolder();
        mhaTblDao.insert(new DalHints(), keyHolder, pojo);
        return (Long) keyHolder.getKey();
    }

    public Long updateOrCreateMha(String mhaName, Long mhaGroupId, Long dcId) throws SQLException {
        MhaTbl mhaTbl = mhaTblDao.queryAll().stream().filter(p -> p.getMhaName().equalsIgnoreCase(mhaName)).findFirst().orElse(null);
        if(null == mhaTbl) {
            return insertMha(mhaName, mhaGroupId, dcId);
        } else if(!mhaGroupId.equals(mhaTbl.getMhaGroupId()) || !dcId.equals(mhaTbl.getDcId()) || BooleanEnum.TRUE.getCode().equals(mhaTbl.getDeleted())) {
            mhaTbl.setMhaGroupId(mhaGroupId);
            mhaTbl.setDcId(dcId);
            mhaTbl.setDeleted(BooleanEnum.FALSE.getCode());
            mhaTblDao.update(mhaTbl);
        }
        return mhaTbl.getId();
    }

    public Long updateOrCreateMha(String mhaName, Long dcId) throws SQLException {
        MhaTbl mhaTbl = mhaTblDao.queryAll().stream().filter(p -> p.getMhaName().equalsIgnoreCase(mhaName)).findFirst().orElse(null);
        if(null == mhaTbl) {
            return insertMha(mhaName, null, dcId);
        } else if(!dcId.equals(mhaTbl.getDcId()) || BooleanEnum.TRUE.getCode().equals(mhaTbl.getDeleted())) {
            mhaTbl.setDcId(dcId);
            mhaTbl.setDeleted(BooleanEnum.FALSE.getCode());
            mhaTblDao.update(mhaTbl);
        }
        return mhaTbl.getId();
    }

    public Long insertResource(String ip, long dcId, ModuleEnum moduleEnum) throws SQLException {
        KeyHolder resourceKeyHolder = new KeyHolder();
        ResourceTbl pojo = createPojo(ip, dcId, moduleEnum);
        resourceTblDao.insert(new DalHints(), resourceKeyHolder, pojo);
        return (Long) resourceKeyHolder.getKey();
    }

    public Long updateOrCreateResource(String ip, Long dcId, ModuleEnum moduleEnum) throws SQLException {
        ResourceTbl resourceTbl = resourceTblDao.queryAll().stream().filter(p -> p.getIp().equalsIgnoreCase(ip)).findFirst().orElse(null);
        if(null == resourceTbl) {
            return insertResource(ip, dcId, moduleEnum);
        } else if (!dcId.equals(resourceTbl.getDcId()) || !((Long)moduleEnum.getAppId()).equals(resourceTbl.getAppId()) || !((Integer)moduleEnum.getCode()).equals(resourceTbl.getType()) || BooleanEnum.TRUE.getCode().equals(resourceTbl.getDeleted())) {
            resourceTbl.setDcId(dcId);
            resourceTbl.setAppId(moduleEnum.getAppId());
            resourceTbl.setType(moduleEnum.getCode());
            resourceTbl.setDeleted(BooleanEnum.FALSE.getCode());
            resourceTblDao.update(resourceTbl);
        }
        return resourceTbl.getId();
    }

    public int insertMachine(String ip, int port, String uuid, BooleanEnum booleanEnum, long mhaId) throws SQLException {
        MachineTbl pojo = createMachinePojo(ip, port, uuid, booleanEnum, mhaId);
        KeyHolder keyHolder = new KeyHolder();
        return machineTblDao.insert(new DalHints(), keyHolder, pojo);
    }

    public int updateOrCreateMachine(String ip, int port, String uuid, BooleanEnum booleanEnum, Long mhaId) throws SQLException {
        MachineTbl machineTbl = machineTblDao.queryAll().stream().filter(p -> p.getIp().equalsIgnoreCase(ip) && p.getPort() == port).findFirst().orElse(null);
        if(null == machineTbl) {
            return insertMachine(ip, port, uuid, booleanEnum, mhaId);
        } else if (!uuid.equals(machineTbl.getUuid()) || !booleanEnum.getCode().equals(machineTbl.getMaster()) || !mhaId.equals(machineTbl.getMhaId()) || BooleanEnum.TRUE.getCode().equals(machineTbl.getDeleted())) {
            machineTbl.setUuid(uuid);
            machineTbl.setMaster(booleanEnum.getCode());
            machineTbl.setMhaId(mhaId);
            machineTbl.setDeleted(BooleanEnum.FALSE.getCode());
            return machineTblDao.update(machineTbl);
        }
        return ZERO_ROWS_AFFECT;
    }

    public Long insertRGroup(long mhaId) throws SQLException {
        KeyHolder keyHolder = new KeyHolder();
        ReplicatorGroupTbl rGroupPojo = createRGroupPojo(mhaId);
        replicatorGroupTblDao.insert(new DalHints(), keyHolder, rGroupPojo);
        return (Long) keyHolder.getKey();
    }

    public Long updateOrCreateRGroup(long mhaId) throws SQLException {
        ReplicatorGroupTbl replicatorGroupTbl = replicatorGroupTblDao.queryAll().stream().filter(p -> p.getMhaId().equals(mhaId)).findFirst().orElse(null);
        if(null == replicatorGroupTbl) {
            logger.info("[[mhaId={}]] insert RGroup", mhaId);
            return insertRGroup(mhaId);
        } else if (BooleanEnum.TRUE.getCode().equals(replicatorGroupTbl.getDeleted())) {
            logger.info("[[mhaId={}]] update RGroup", mhaId);
            replicatorGroupTbl.setDeleted(BooleanEnum.FALSE.getCode());
            replicatorGroupTblDao.update(replicatorGroupTbl);
        }
        return replicatorGroupTbl.getId();
    }

    public Long insertAGroup(long replicatorGroupId, long mhaId, String includedDbs, int applyMode, String nameFilter, String nameMapping) throws SQLException {
        KeyHolder keyHolder = new KeyHolder();
        ApplierGroupTbl aGroupPojo = createAGroupPojo(replicatorGroupId, mhaId, includedDbs, applyMode, nameFilter, nameMapping);
        applierGroupTblDao.insert(new DalHints(), keyHolder, aGroupPojo);
        return (Long) keyHolder.getKey();
    }

    public Long updateOrCreateAGroup(long replicatorGroupId, long mhaId, String includedDbs, int applyMode, String nameFilter, String nameMapping) throws SQLException {
        logger.debug("updateOrCreateAGroup: {}-{}-{}-{}", replicatorGroupId, mhaId, includedDbs, applyMode);
        ApplierGroupTbl applierGroupTbl = applierGroupTblDao.queryAll().stream().filter(p -> p.getReplicatorGroupId().equals(replicatorGroupId) && p.getMhaId().equals(mhaId)).findFirst().orElse(null);
        if(null == applierGroupTbl) {
            logger.info("[[mhaId={}]] insert AGroup", mhaId);
            return insertAGroup(replicatorGroupId, mhaId, includedDbs, applyMode, nameFilter, nameMapping);
        } else if (BooleanEnum.TRUE.getCode().equals(applierGroupTbl.getDeleted())
                || !(includedDbs == null ? applierGroupTbl.getIncludedDbs() == null : includedDbs.equalsIgnoreCase(applierGroupTbl.getIncludedDbs()))
                || applyMode != applierGroupTbl.getApplyMode()
                || !(nameFilter == null ? applierGroupTbl.getNameFilter() == null : nameFilter.equalsIgnoreCase(applierGroupTbl.getNameFilter()))
                || !(nameMapping == null ? applierGroupTbl.getNameMapping() == null : nameMapping.equalsIgnoreCase(applierGroupTbl.getNameMapping()))
        ) {
            logger.info("[[mhaId={}]] update AGroup, included dbs is: {}, apply mode is: {}", mhaId, includedDbs, applyMode);
            applierGroupTbl.setDeleted(BooleanEnum.FALSE.getCode());
            applierGroupTbl.setIncludedDbs((includedDbs == null || includedDbs.length() == 0) ? null : includedDbs);
            applierGroupTbl.setApplyMode(applyMode);
            applierGroupTbl.setNameFilter((StringUtils.isBlank(nameFilter)) ? null : nameFilter);
            applierGroupTbl.setNameMapping((StringUtils.isBlank(nameMapping)) ? null : nameMapping);
            DalHints dalHints=new DalHints();
            dalHints.updateNullField();
            applierGroupTblDao.update(dalHints, applierGroupTbl);
        }
        return applierGroupTbl.getId();
    }

    public void insertRoute(Long routeOrgId, Long srcDcId, Long dstDcId, String srcProxyIds, String relayProxyIds, String dstProxyIds, String tag) throws SQLException {
        RouteTbl routeTbl = createRoutePojo(routeOrgId, srcDcId, dstDcId, srcProxyIds, relayProxyIds, dstProxyIds, tag);
        routeTblDao.insert(routeTbl);
    }

    public void updateOrCreateRoute(Long routeOrgId, Long srcDcId, Long dstDcId, String srcProxyIds, String relayProxyIds, String dstProxyIds, String tag) throws SQLException {
        RouteTbl routeTbl = routeTblDao.queryAll().stream().filter(p -> routeOrgId.equals(p.getRouteOrgId()) && tag.equalsIgnoreCase(p.getTag()) && srcDcId.equals(p.getSrcDcId()) && dstDcId.equals(p.getDstDcId())).findFirst().orElse(null);
        if(null == routeTbl) {
            insertRoute(routeOrgId, srcDcId, dstDcId, srcProxyIds, relayProxyIds, dstProxyIds, tag);
        } else if (BooleanEnum.TRUE.getCode().equals(routeTbl.getDeleted())
                || !srcProxyIds.equalsIgnoreCase(routeTbl.getSrcProxyIds())
                || !dstProxyIds.equalsIgnoreCase(routeTbl.getDstProxyIds())
                || !relayProxyIds.equalsIgnoreCase(routeTbl.getOptionalProxyIds())) {
            routeTbl.setDeleted(BooleanEnum.FALSE.getCode());
            routeTbl.setSrcProxyIds(srcProxyIds);
            routeTbl.setDstProxyIds(dstProxyIds);
            routeTbl.setOptionalProxyIds(relayProxyIds);
            routeTblDao.update(routeTbl);
        }
    }

    public void updateOrCreateProxy(ProxyTbl pojo) throws SQLException {
        ProxyTbl proxyTbl = proxyTblDao.queryAll().stream().filter(p -> pojo.getUri().equalsIgnoreCase(p.getUri())).findFirst().orElse(null);
        if(null == proxyTbl) {
            if(BooleanEnum.FALSE.getCode().equals(pojo.getDeleted())) {
                proxyTblDao.insert(pojo);
            }
        } else if (!proxyTbl.getDcId().equals(pojo.getDcId()) || !proxyTbl.getActive().equals(pojo.getActive()) || !proxyTbl.getMonitorActive().equals(pojo.getMonitorActive()) && !proxyTbl.getDeleted().equals(pojo.getDeleted())) {
            proxyTblDao.update(pojo);
        }
    }

    public void insertClusterManager(int port, long resourceId, boolean master) throws SQLException {
        ClusterManagerTbl cmPojo = createPojo(port, resourceId, master);
        clusterManagerTblDao.insert(cmPojo);
    }

    public void updateOrCreateCM(int port, long resourceId, Boolean master) throws SQLException {
        ClusterManagerTbl clusterManagerTbl = clusterManagerTblDao.queryAll().stream().filter(p -> p.getPort().equals(port) && p.getResourceId().equals(resourceId)).findFirst().orElse(null);
        if(null == clusterManagerTbl) {
            insertClusterManager(port, resourceId, master);
        } else {
            Integer isMaster = master ? BooleanEnum.TRUE.getCode() : BooleanEnum.FALSE.getCode();
            if (!isMaster.equals(clusterManagerTbl.getMaster()) || BooleanEnum.TRUE.getCode().equals(clusterManagerTbl.getDeleted())) {
                clusterManagerTbl.setMaster(isMaster);
                clusterManagerTbl.setDeleted(BooleanEnum.FALSE.getCode());
                clusterManagerTblDao.update(clusterManagerTbl);
            }
        }
    }

    public void insertZk(int port, long resourceId) throws SQLException {
        ZookeeperTbl zkPojo = createPojo(port, resourceId);
        zookeeperTblDao.insert(zkPojo);
    }

    public void updateOrCreateZk(int port, long resourceId) throws SQLException {
        ZookeeperTbl zookeeperTbl = zookeeperTblDao.queryAll().stream().filter(p -> p.getPort().equals(port) && p.getResourceId().equals(resourceId)).findFirst().orElse(null);
        if(null == zookeeperTbl) {
            insertZk(port, resourceId);
        } else if (BooleanEnum.TRUE.getCode().equals(zookeeperTbl.getDeleted())) {
            zookeeperTbl.setDeleted(BooleanEnum.FALSE.getCode());
            zookeeperTblDao.update(zookeeperTbl);
        }
    }

    public Long insertReplicator(int port, int applierPort, String gtidInit, long resourceId, long replicatorGroupId, BooleanEnum master) throws SQLException {
        ReplicatorTbl pojo = createReplicatorPojo(port, applierPort, gtidInit, resourceId, replicatorGroupId, master);
        KeyHolder keyHolder = new KeyHolder();
        replicatorTblDao.insert(new DalHints(), keyHolder, pojo);
        return (Long) keyHolder.getKey();
    }

    public Long updateOrCreateReplicator(Integer port, int applierPort, String gtidInit, long resourceId, Long replicatorGroupId, BooleanEnum master) throws SQLException {
        ReplicatorTbl replicatorTbl = replicatorTblDao.queryAll().stream().filter(p -> p.getResourceId().equals(resourceId) && p.getApplierPort().equals(applierPort)).findFirst().orElse(null);
        if(null == replicatorTbl) {
            return insertReplicator(port, applierPort, gtidInit, resourceId, replicatorGroupId, master);
        } else if (!port.equals(replicatorTbl.getPort()) || !gtidInit.equalsIgnoreCase(replicatorTbl.getGtidInit()) || !replicatorGroupId.equals(replicatorTbl.getRelicatorGroupId()) || !master.getCode().equals(replicatorTbl.getMaster()) || BooleanEnum.TRUE.getCode().equals(replicatorTbl.getDeleted())) {
            replicatorTbl.setPort(port);
            replicatorTbl.setMaster(master.getCode());
            replicatorTbl.setRelicatorGroupId(replicatorGroupId);
            replicatorTbl.setGtidInit(gtidInit);
            replicatorTbl.setDeleted(BooleanEnum.FALSE.getCode());
            replicatorTblDao.update(replicatorTbl);
        }
        return replicatorTbl.getId();
    }

    public Long insertApplier(int port, String gtidInit, Long resourceId, Long applierGroupId) throws SQLException {
        ApplierTbl pojo = createApplierPojo(port, gtidInit, resourceId, applierGroupId);
        KeyHolder keyHolder = new KeyHolder();
        applierTblDao.insert(new DalHints(), keyHolder, pojo);
        return (Long) keyHolder.getKey();
    }

    public Long updateOrCreateApplier(Integer port, String gtidInit, Long resourceId, Long applierGroupId, BooleanEnum master) throws SQLException {
        ApplierTbl applierTbl = applierTblDao.queryAll().stream().filter(p -> p.getResourceId().equals(resourceId) && p.getApplierGroupId().equals(applierGroupId)).findFirst().orElse(null);
        if(null == applierTbl) {
            return insertApplier(port, gtidInit, resourceId, applierGroupId);
        } else if (!port.equals(applierTbl.getPort()) || !gtidInit.equalsIgnoreCase(applierTbl.getGtidInit()) || !master.getCode().equals(applierTbl.getMaster()) || BooleanEnum.TRUE.getCode().equals(applierTbl.getDeleted())) {
            applierTbl.setPort(port);
            applierTbl.setGtidInit(gtidInit);
            applierTbl.setMaster(master.getCode());
            applierTbl.setDeleted(BooleanEnum.FALSE.getCode());
            applierTblDao.update(applierTbl);
        }
        return applierTbl.getId();
    }

    public int updateDataConsistencyMonitor(DataConsistencyMonitorTbl dataConsistencyMonitorTbl) throws SQLException {
        return dataConsistencyMonitorTblDao.update(dataConsistencyMonitorTbl);
    }

    public int updateReplicatorGroupTbl(ReplicatorGroupTbl replicatorGroupTbl) throws SQLException {
        return replicatorGroupTblDao.update(replicatorGroupTbl);
    }

    public void insertDataConsistencyMonitor(int mhaId, String schema, String table, String tableKey, String onUpdate) throws SQLException {
        DataConsistencyMonitorTbl dataConsistencyMonitorTbl = new DataConsistencyMonitorTbl();
        dataConsistencyMonitorTbl.setMhaId(mhaId);
        dataConsistencyMonitorTbl.setMonitorSchemaName(schema);
        dataConsistencyMonitorTbl.setMonitorTableName(table);
        dataConsistencyMonitorTbl.setMonitorTableKey(tableKey);
        dataConsistencyMonitorTbl.setMonitorTableOnUpdate(onUpdate);
        dataConsistencyMonitorTblDao.insert(dataConsistencyMonitorTbl);
    }

    public int insertDdlHistory(String mhaName, String ddl, int queryType, String schemaName, String tableName) throws Throwable {
        Long mhaId = getId(TableEnum.MHA_TABLE, mhaName);
        DdlHistoryTbl pojo = createDdlHistoryPojo(mhaId, ddl, queryType, schemaName, tableName);
        KeyHolder keyHolder = new KeyHolder();
        return ddlHistoryTblDao.insert(new DalHints(), keyHolder, pojo);
    }

    public int insertDataInconsistencyHistory(String monitorSchemaName, String monitorTableName, String monitorTableKey, String monitorTableKeyValue, Long mhaGroupId, SourceTypeEnum sourceTypeEnum) throws SQLException {
        DataInconsistencyHistoryTbl pojo = createDataInconsistencyHistoryPojo(monitorSchemaName, monitorTableName, monitorTableKey, monitorTableKeyValue, mhaGroupId, sourceTypeEnum);
        KeyHolder keyHolder = new KeyHolder();
        return dataInconsistencyHistoryTblDao.insert(new DalHints(), keyHolder, pojo);
    }

    public int insertUnitRouteVerificationHistory(String schemaName, String tableName, String gtid, String querySql, String expectedDc, String actualDc, String columns, String beforeValues, String afterValues, String uidName, Integer ucsStrategyId, Long mhaGroupId, Timestamp executeTime) throws SQLException {
        UnitRouteVerificationHistoryTbl pojo = createUnitRouteVerificationHistoryPojo(schemaName, tableName, gtid, querySql, expectedDc, actualDc, columns, beforeValues, afterValues, uidName, ucsStrategyId, mhaGroupId, executeTime);
        return unitRouteVerificationHistoryTblDao.insert(pojo);
    }

    public BuTbl createBuPojo(String buName) {
        BuTbl daoPojo = new BuTbl();
        daoPojo.setBuName(buName);
        return daoPojo;
    }

    public ClusterMhaMapTbl createClusterMhaMapPojo(long clusterId, long mhaId) {
        ClusterMhaMapTbl daoPojo = new ClusterMhaMapTbl();
        daoPojo.setClusterId(clusterId);
        daoPojo.setMhaId(mhaId);
        return daoPojo;
    }

    public DcTbl createDcPojo(String dcName) {
        DcTbl daoPojo = new DcTbl();
        daoPojo.setDcName(dcName);
        return daoPojo;
    }

    public ClusterTbl createClusterPojo(String clusterName, Long clusterAppId, Long buId) {
        ClusterTbl daoPojo = new ClusterTbl();
        daoPojo.setClusterName(clusterName);
        daoPojo.setClusterAppId(clusterAppId);
        daoPojo.setBuId(buId);
        return daoPojo;
    }

    public MhaGroupTbl createMhaGroupPojo(BooleanEnum drcStatus, EstablishStatusEnum establishStatusEnum, String readUser, String readPassword, String writeUser, String writePassword, String monitorUser, String monitorPassword) {
        MhaGroupTbl daoPojo = new MhaGroupTbl();
        daoPojo.setDrcStatus(drcStatus.getCode());
        daoPojo.setDrcEstablishStatus(establishStatusEnum.getCode());
        daoPojo.setReadUser(readUser);
        daoPojo.setReadPassword(readPassword);
        daoPojo.setWriteUser(writeUser);
        daoPojo.setWritePassword(writePassword);
        daoPojo.setMonitorUser(monitorUser);
        daoPojo.setMonitorPassword(monitorPassword);
        return daoPojo;
    }

    public MhaTbl createMhaPojo(String mhaName, Long mhaGroupId, Long dcId) {
        MhaTbl daoPojo = new MhaTbl();
        daoPojo.setMhaName(mhaName);
        daoPojo.setMhaGroupId(mhaGroupId);
        daoPojo.setDcId(dcId);
        return daoPojo;
    }

    public ResourceTbl createPojo(String ip, long dcId, ModuleEnum moduleEnum) {
        ResourceTbl daoPojo = new ResourceTbl();
        daoPojo.setAppId(moduleEnum.getAppId());
        daoPojo.setIp(ip);
        daoPojo.setDcId(dcId);
        daoPojo.setType(moduleEnum.getCode());
        return daoPojo;
    }

    public MachineTbl createMachinePojo(String ip, int port, String uuid, BooleanEnum booleanEnum, long mhaId) {
        MachineTbl daoPojo = new MachineTbl();
        daoPojo.setIp(ip);
        daoPojo.setPort(port);
        daoPojo.setMaster(booleanEnum.getCode());
        daoPojo.setUuid(uuid);
        daoPojo.setMhaId(mhaId);
        return daoPojo;
    }

    public ReplicatorGroupTbl createRGroupPojo(long mhaId) {
        ReplicatorGroupTbl daoPojo = new ReplicatorGroupTbl();
        daoPojo.setMhaId(mhaId);
        return daoPojo;
    }

    public ApplierGroupTbl createAGroupPojo(Long replicatorGroupId, Long mhaId, String includedDbs, int applyMode, String nameFilter, String nameMapping) {
        ApplierGroupTbl daoPojo = new ApplierGroupTbl();
        daoPojo.setReplicatorGroupId(replicatorGroupId);
        daoPojo.setMhaId(mhaId);
        daoPojo.setIncludedDbs(includedDbs);
        daoPojo.setApplyMode(applyMode);
        daoPojo.setNameFilter(nameFilter);
        daoPojo.setNameMapping(nameMapping);
        return daoPojo;
    }

    public RouteTbl createRoutePojo(Long routeOrgId, Long srcDcId, Long dstDcId, String srcProxyIds, String relayProxyIds, String dstProxyIds, String tag) {
        RouteTbl routeTbl = new RouteTbl();
        routeTbl.setRouteOrgId(routeOrgId);
        routeTbl.setSrcDcId(srcDcId);
        routeTbl.setDstDcId(dstDcId);
        routeTbl.setSrcProxyIds(srcProxyIds);
        routeTbl.setOptionalProxyIds(relayProxyIds);
        routeTbl.setDstProxyIds(dstProxyIds);
        routeTbl.setTag(tag);
        return routeTbl;
    }

    public ClusterManagerTbl createPojo(int port, long resourceId, boolean master) {
        ClusterManagerTbl daoPojo = new ClusterManagerTbl();
        daoPojo.setPort(port);
        daoPojo.setResourceId(resourceId);
        daoPojo.setMaster(master ? BooleanEnum.TRUE.getCode() : BooleanEnum.FALSE.getCode());
        return daoPojo;
    }

    public ZookeeperTbl createPojo(int port, long resourceId) {
        ZookeeperTbl daoPojo = new ZookeeperTbl();
        daoPojo.setPort(port);
        daoPojo.setResourceId(resourceId);
        return daoPojo;
    }

    protected ReplicatorTbl createReplicatorPojo(int port, int applierPort, String gtidInit, long resourceId, long replicatorGroupId, BooleanEnum booleanEnum) {
        ReplicatorTbl daoPojo = new ReplicatorTbl();
        daoPojo.setPort(port);
        daoPojo.setApplierPort(applierPort);
        daoPojo.setResourceId(resourceId);
        daoPojo.setGtidInit(gtidInit);
        daoPojo.setMaster(booleanEnum.getCode());
        daoPojo.setRelicatorGroupId(replicatorGroupId);
        return daoPojo;
    }

    protected ApplierTbl createApplierPojo(int port, String gtidInit, Long resourceId, Long applierGroupId) {
        ApplierTbl daoPojo = new ApplierTbl();
        daoPojo.setPort(port);
        daoPojo.setGtidInit(gtidInit);
        daoPojo.setResourceId(resourceId);
        daoPojo.setApplierGroupId(applierGroupId);
        return daoPojo;
    }

    public DdlHistoryTbl createDdlHistoryPojo(long mhaId, String ddl, int queryType, String schemaName, String tableName) {
        DdlHistoryTbl daoPojo = new DdlHistoryTbl();
        daoPojo.setMhaId(mhaId);
        daoPojo.setDdl(ddl);
        daoPojo.setQueryType(queryType);
        daoPojo.setSchemaName(schemaName);
        daoPojo.setTableName(tableName);
        return daoPojo;
    }

    public DataInconsistencyHistoryTbl createDataInconsistencyHistoryPojo(String monitorSchemaName, String monitorTableName, String monitorTableKey, String monitorTableKeyValue, Long mhaGroupId, SourceTypeEnum sourceTypeEnum) {
        DataInconsistencyHistoryTbl daoPojo = new DataInconsistencyHistoryTbl();
        daoPojo.setMonitorSchemaName(monitorSchemaName);
        daoPojo.setMonitorTableName(monitorTableName);
        daoPojo.setMonitorTableKey(monitorTableKey);
        daoPojo.setMonitorTableKeyValue(monitorTableKeyValue);
        daoPojo.setMhaGroupId(mhaGroupId);
        daoPojo.setSourceType(sourceTypeEnum.getCode());
        return daoPojo;
    }

    public UnitRouteVerificationHistoryTbl createUnitRouteVerificationHistoryPojo(String schemaName, String tableName, String gtid, String querySql, String expectedDc, String actualDc, String columns, String beforeValues, String afterValues, String uidName, Integer ucsStrategyId, Long mhaGroupId, Timestamp executeTime) {
        UnitRouteVerificationHistoryTbl pojo = new UnitRouteVerificationHistoryTbl();
        pojo.setSchemaName(schemaName);
        pojo.setTableName(tableName);
        pojo.setGtid(gtid);
        pojo.setQuerySql(querySql);
        pojo.setExpectedDc(expectedDc);
        pojo.setActualDc(actualDc);
        pojo.setColumns(columns);
        pojo.setBeforeValues(beforeValues);
        pojo.setAfterValues(afterValues);
        pojo.setUidName(uidName);
        pojo.setUcsStrategyId(ucsStrategyId);
        pojo.setMhaGroupId(mhaGroupId);
        pojo.setExecuteTime(executeTime);
        return pojo;
    }

    public BuTblDao getBuTblDao() { return buTblDao; }
    public RouteTblDao getRouteTblDao() { return routeTblDao; }
    public ProxyTblDao getProxyTblDao() { return proxyTblDao; }
    public ClusterMhaMapTblDao getClusterMhaMapTblDao() { return clusterMhaMapTblDao; }
    public GroupMappingTblDao getGroupMappingTblDao() { return groupMappingTblDao; }
    public DcTblDao getDcTblDao() { return dcTblDao; }
    public ClusterTblDao getClusterTblDao() { return clusterTblDao; }
    public MhaGroupTblDao getMhaGroupTblDao() { return mhaGroupTblDao; }
    public MhaTblDao getMhaTblDao() { return mhaTblDao; }
    public ResourceTblDao getResourceTblDao() { return resourceTblDao; }
    public MachineTblDao getMachineTblDao() { return machineTblDao; }
    public ReplicatorGroupTblDao getReplicatorGroupTblDao() { return replicatorGroupTblDao; }
    public ApplierGroupTblDao getApplierGroupTblDao() { return applierGroupTblDao; }
    public ClusterManagerTblDao getClusterManagerTblDao() { return clusterManagerTblDao; }
    public ZookeeperTblDao getZookeeperTblDao() { return zookeeperTblDao; }
    public ReplicatorTblDao getReplicatorTblDao() { return replicatorTblDao; }
    public ApplierTblDao getApplierTblDao() { return applierTblDao; }
    public DataConsistencyMonitorTblDao getDataConsistencyMonitorTblDao() { return dataConsistencyMonitorTblDao; }
    public DataInconsistencyHistoryTblDao getDataInconsistencyHistoryTblDao() { return dataInconsistencyHistoryTblDao; }
    public DdlHistoryTblDao getDdlHistoryTblDao() { return ddlHistoryTblDao; }
    public UnitRouteVerificationHistoryTblDao getUnitRouteVerificationHistoryTblDao() { return unitRouteVerificationHistoryTblDao; }
    public DalQueryDao getDalQueryDao() { return dalQueryDao; }
}
