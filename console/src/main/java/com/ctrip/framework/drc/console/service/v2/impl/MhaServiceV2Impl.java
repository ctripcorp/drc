package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.dao.*;
import com.ctrip.framework.drc.console.dao.entity.*;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaDbMappingTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.MhaDbMappingTblDao;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.dto.MhaInstanceGroupDto;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.ReadableErrorDefEnum;
import com.ctrip.framework.drc.console.exception.ConsoleException;
import com.ctrip.framework.drc.console.monitor.delay.config.v2.MetaProviderV2;
import com.ctrip.framework.drc.console.param.v2.MhaQuery;
import com.ctrip.framework.drc.console.pojo.domain.DcDo;
import com.ctrip.framework.drc.console.service.impl.api.ApiContainer;
import com.ctrip.framework.drc.console.service.v2.MetaInfoServiceV2;
import com.ctrip.framework.drc.console.service.v2.MhaServiceV2;
import com.ctrip.framework.drc.console.service.v2.external.dba.DbaApiService;
import com.ctrip.framework.drc.console.service.v2.external.dba.response.ClusterInfoDto;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.console.utils.EnvUtils;
import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.console.vo.check.DrcBuildPreCheckVo;
import com.ctrip.framework.drc.console.vo.request.MhaQueryDto;
import com.ctrip.framework.drc.core.entity.Applier;
import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Dc;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.entity.Messenger;
import com.ctrip.framework.drc.core.entity.Replicator;
import com.ctrip.framework.drc.core.monitor.enums.ModuleEnum;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.service.ops.OPSApiService;
import com.ctrip.framework.drc.core.service.statistics.traffic.HickWallMhaReplicationDelayEntity;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.KeyHolder;
import com.ctrip.platform.dal.dao.annotation.DalTransactional;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Lists;
import java.util.Map.Entry;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by yongnian
 * 2023/7/26 14:09
 */
@Service
public class MhaServiceV2Impl implements MhaServiceV2 {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private OPSApiService opsApiServiceImpl = ApiContainer.getOPSApiServiceImpl();
    
    @Autowired
    private MhaTblV2Dao mhaTblV2Dao;
    @Autowired
    private MetaInfoServiceV2 metaInfoServiceV2;
    @Autowired
    private ReplicatorGroupTblDao replicatorGroupTblDao;
    @Autowired
    private ReplicatorTblDao replicatorTblDao;
    @Autowired
    private ResourceTblDao resourceTblDao;
    @Autowired
    private DcTblDao dcTblDao;
    @Autowired
    private MachineTblDao machineTblDao;
    @Autowired
    private MessengerGroupTblDao messengerGroupTblDao;
    @Autowired
    private MessengerTblDao messengerTblDao;
    @Autowired
    private DomainConfig domainConfig;
    @Autowired
    private DbaApiService dbaApiService;
    @Autowired
    private DbTblDao dbTblDao;
    @Autowired
    private MhaDbMappingTblDao mhaDbMappingTblDao;
    @Autowired
    private MetaProviderV2 metaProviderV2;


    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public Map<Long, MhaTblV2> query(String containMhaName, Long buId, Long regionId) {
        try {
            MhaQuery mhaQuery = new MhaQuery();
            mhaQuery.setContainMhaName(containMhaName);
            mhaQuery.setBuId(buId);
            if (regionId != null && regionId > 0) {
                List<DcDo> dcDos = metaInfoServiceV2.queryAllDcWithCache();
                List<Long> dcIdList = dcDos.stream()
                        .filter(e -> regionId.equals(e.getRegionId()))
                        .map(DcDo::getDcId)
                        .collect(Collectors.toList());
                if (CollectionUtils.isEmpty(dcIdList)) {
                    return Collections.emptyMap();
                }
                mhaQuery.setDcIdList(dcIdList);
            }

            if (mhaQuery.emptyQueryCondition()) {
                return Collections.emptyMap();
            }
            List<MhaTblV2> mhaTblV2List = mhaTblV2Dao.query(mhaQuery);
            return mhaTblV2List.stream().collect(Collectors.toMap(MhaTblV2::getId, Function.identity(), (e1, e2) -> e1));
        } catch (SQLException e) {
            logger.error("queryMhaByName exception", e);
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
    }

    @Override
    public Map<Long, MhaTblV2> query(MhaQueryDto mha) {
        if (mha == null || !mha.isConditionalQuery()) {
            return Collections.emptyMap();
        }
        return query(StringUtils.trim(mha.getName()), mha.getBuId(), mha.getRegionId());
    }

    @Override
    public Map<Long, MhaTblV2> queryMhaByIds(List<Long> mhaIds) {
        if (CollectionUtils.isEmpty(mhaIds)) {
            return Collections.emptyMap();
        }
        try {
            List<MhaTblV2> mhaTblV2List = mhaTblV2Dao.queryByIds(mhaIds);
            return mhaTblV2List.stream().collect(Collectors.toMap(MhaTblV2::getId, Function.identity(), (e1, e2) -> e1));
        } catch (SQLException e) {
            logger.error("queryByMhaNames exception", e);
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
    }

    @Override
    public List<MhaTblV2> queryRelatedMhaByDbName(List<String> dbNames) throws SQLException {
        if (CollectionUtils.isEmpty(dbNames)) {
            return Collections.emptyList();
        }
        if (dbNames.size() >= 100) {
            throw ConsoleExceptionUtils.message("query illegal: db num exceed 100: " + dbNames.size());
        }
        List<MhaTblV2> mhaTblV2s = new ArrayList<>();
        List<String> queriedMhaNames = new ArrayList<>();

        // from local
        List<DbTbl> dbTbls = dbTblDao.queryByDbNames(dbNames);
        if (!CollectionUtils.isEmpty(dbTbls)) {
            List<Long> dbIds = dbTbls.stream().map(DbTbl::getId).collect(Collectors.toList());
            List<MhaDbMappingTbl> mhaDbMappingTbls = mhaDbMappingTblDao.queryByDbIds(dbIds);
            List<Long> mhaIds = mhaDbMappingTbls.stream().map(MhaDbMappingTbl::getMhaId).collect(Collectors.toList());
            if (!CollectionUtils.isEmpty(mhaIds)) {
                mhaTblV2s.addAll(mhaTblV2Dao.queryByIds(mhaIds));
                queriedMhaNames.addAll(mhaTblV2s.stream().map(MhaTblV2::getMhaName).collect(Collectors.toList()));
            }
        }

        // from dba api
        List<String> mhaNames = dbNames.stream()
                .flatMap(dbName -> this.queryMhaFromDbaApi(dbName).stream())
                .filter(e -> !queriedMhaNames.contains(e))
                .distinct()
                .collect(Collectors.toList());
        if (!CollectionUtils.isEmpty(mhaNames)) {
            mhaTblV2s.addAll(mhaTblV2Dao.queryByMhaNames(mhaNames));
        }
        return mhaTblV2s;
    }

    public List<String> queryMhaFromDbaApi(String dbName) {
        try {
            List<ClusterInfoDto> clusterInfoDtoList = dbaApiService.getDatabaseClusterInfo(dbName);
            return clusterInfoDtoList.stream().map(ClusterInfoDto::getClusterName).collect(Collectors.toList());
        } catch (ConsoleException e) {
            if (e.getMessage().contains("empty result")) {
                return Collections.emptyList();
            }
            throw e;
        }
    }


    @Override
    public List<String> getMhaReplicators(String mhaName) throws Exception {
        MhaTblV2 mhaTblV2 = mhaTblV2Dao.queryByMhaName(mhaName);
        if (mhaTblV2 == null) {
            logger.info("mha: {} not exist", mhaName);
            return new ArrayList<>();
        }
        ReplicatorGroupTbl replicatorGroupTbl = replicatorGroupTblDao.queryByMhaId(mhaTblV2.getId(), BooleanEnum.FALSE.getCode());
        if (replicatorGroupTbl == null) {
            logger.info("replicatorGroupTbl not exist, mhaName: {}", mhaName);
            return new ArrayList<>();
        }
        List<ReplicatorTbl> replicatorTbls = replicatorTblDao.queryByRGroupIds(Lists.newArrayList(replicatorGroupTbl.getId()), BooleanEnum.FALSE.getCode());
        if (CollectionUtils.isEmpty(replicatorTbls)) {
            return new ArrayList<>();
        }
        List<Long> resourceIds = replicatorTbls.stream().map(ReplicatorTbl::getResourceId).collect(Collectors.toList());
        List<ResourceTbl> resourceTbls = resourceTblDao.queryByIds(resourceIds);
        List<String> replicatorIps = resourceTbls.stream().map(ResourceTbl::getIp).collect(Collectors.toList());
        return replicatorIps;
    }

    @Override
    public List<String> getMhaAvailableResource(String mhaName, int type) throws Exception {
        MhaTblV2 mhaTblV2 = mhaTblV2Dao.queryByMhaName(mhaName);
        if (mhaTblV2 == null) {
            logger.info("mha: {} not exist", mhaName);
            return new ArrayList<>();
        }

        if (type != ModuleEnum.REPLICATOR.getCode() && type != ModuleEnum.APPLIER.getCode()) {
            logger.info("resource type: {} can only be replicator or applier", type);
            return new ArrayList<>();
        }
        DcTbl dcTbl = dcTblDao.queryById(mhaTblV2.getDcId());
        List<Long> dcIds = dcTblDao.queryByRegionName(dcTbl.getRegionName()).stream().map(DcTbl::getId).collect(Collectors.toList());
        List<ResourceTbl> resourceTbls = resourceTblDao.queryByDcAndType(dcIds, type);
        List<String> ips = resourceTbls.stream().map(ResourceTbl::getIp).collect(Collectors.toList());
        return ips;
    }

    @Override
    public String getMysqlUuid(String mhaName, String ip, int port, boolean master) throws Exception {
        MhaTblV2 mhaTblV2 = mhaTblV2Dao.queryByMhaName(mhaName, BooleanEnum.FALSE.getCode());
        if (mhaTblV2 == null) {
            throw ConsoleExceptionUtils.message(String.format("mha: %s not exist", mhaName));
        }
        String uuid = MySqlUtils.getUuid(ip, port, mhaTblV2.getReadUser(), mhaTblV2.getReadPassword(), master);
        return uuid;
    }

    @Override
    public boolean recordMhaInstances(MhaInstanceGroupDto dto) throws Exception {
        String mhaName = dto.getMhaName();
        MhaTblV2 mhaTblV2 = mhaTblV2Dao.queryByMhaName(mhaName, BooleanEnum.FALSE.getCode());
        if (null == mhaTblV2) {
            logger.info("mha({}) null", mhaName);
            return false;
        }
        Long mhaId = mhaTblV2.getId();
        List<MachineTbl> currentMachineTbls = machineTblDao.queryByMhaId(mhaId, BooleanEnum.FALSE.getCode());
        if (dto.getMaster() != null) {
            logger.info("add master machine-{} in mha-{}", dto.getMaster(), dto.getMhaName());
            return checkMasterMachineMatch(dto, currentMachineTbls, mhaTblV2);
        } else if (dto.getSlaves() != null) {
            logger.info("add slave machine-{} in mha-{}", dto.getMaster(), dto.getMhaName());
            checkSlaveMachines(dto, currentMachineTbls, mhaTblV2);
            return true;
        }
        return false;
    }

    private boolean checkMasterMachineMatch(MhaInstanceGroupDto dto, List<MachineTbl> currentMachineTbls, MhaTblV2 mhaTblV2) throws Exception {
        MhaInstanceGroupDto.MySQLInstance master = dto.getMaster();
        if (currentMachineTbls.size() == 0) {
            insertMasterMachine(mhaTblV2, master.getIp(), master.getPort(), master.getUuid());
            return true;
        } else {
            // check: alert if master not exist in current or current master does not match
            boolean masterMatch = false;
            for (MachineTbl machineTbl : currentMachineTbls) {
                if (BooleanEnum.TRUE.getCode().equals(machineTbl.getMaster()) && machineTbl.getIp().equalsIgnoreCase(master.getIp()) && machineTbl.getPort().equals(master.getPort())) {
                    masterMatch = true;
                    break;
                }
            }
            if (!masterMatch) {
                logger.error("new master {} {}:{} after init", mhaTblV2.getMhaName(), master.getIp(), master.getPort());
                DefaultEventMonitorHolder.getInstance().logEvent("DRC.mysql.instance.unnotified", mhaTblV2.getMhaName());
            }
            return masterMatch;
        }
    }

    private void checkSlaveMachines(MhaInstanceGroupDto dto, List<MachineTbl> currentMachineTbls, MhaTblV2 mhaTblV2) throws Exception {
        List<String> currentMachineIps = currentMachineTbls.stream().map(MachineTbl::getIp).collect(Collectors.toList());
        List<MhaInstanceGroupDto.MySQLInstance> slaves = dto.getSlaves();
        for (MhaInstanceGroupDto.MySQLInstance slave : slaves) {
            String ip = slave.getIp();
            if (!currentMachineIps.contains(ip)) {
                int port = slave.getPort();
                insertSlaveMachine(mhaTblV2, ip, port, slave.getUuid());
            }
        }
    }

    private int insertMasterMachine(MhaTblV2 mhaTblV2, String ip, int port, String suppliedUuid) throws Exception {
        String mhaName = mhaTblV2.getMhaName();
        long mhaId = mhaTblV2.getId();
        logger.info("[[mha={}]]no such master {}:{}, try insert", mhaTblV2.getMhaName(), ip, port);
        String uuid = StringUtils.isNotBlank(suppliedUuid) ? suppliedUuid : MySqlUtils.getUuid(ip, port, mhaTblV2.getMonitorUser(), mhaTblV2.getMonitorPassword(), true);
        if (null == uuid) {
            logger.error("[[mha={}]]cannot get uuid for {}:{}, do nothing", mhaTblV2.getMhaName(), ip, port);
            DefaultEventMonitorHolder.getInstance().logEvent("DRC.mysql.master.insert.fail." + mhaName, ip);
            throw ConsoleExceptionUtils.message(mhaName + " cannot get uuid " + ip);
        }
        logger.info("[[mha={}]] insert master {}:{}", mhaName, ip, port);
        DefaultEventMonitorHolder.getInstance().logEvent("DRC.mysql.master.insert." + mhaName, ip);
        return insertMachine(ip, port, uuid, BooleanEnum.TRUE.getCode(), mhaId);
    }

    private int insertSlaveMachine(MhaTblV2 mhaTblV2, String ip, int port, String suppliedUuid) throws Exception {
        String mhaName = mhaTblV2.getMhaName();
        long mhaId = mhaTblV2.getId();
        logger.info("[[mha={}]]no such slave {}:{}, try insert", mhaName, ip, port);
        String uuid = StringUtils.isNotBlank(suppliedUuid) ? suppliedUuid : MySqlUtils.getUuid(ip, port, mhaTblV2.getMonitorUser(), mhaTblV2.getMonitorPassword(), false);
        if (null == uuid) {
            logger.error("[[mha={}]]cannot get uuid for {}:{}, do nothing", mhaName, ip, port);
            DefaultEventMonitorHolder.getInstance().logEvent("DRC.mysql.slave.insert.fail." + mhaName, ip);
            throw ConsoleExceptionUtils.message(mhaName + " cannot get uuid " + ip);
        }
        logger.info("[[mha={}]] insert slave {}:{}", mhaName, ip, port);
        DefaultEventMonitorHolder.getInstance().logEvent("DRC.mysql.slave.insert." + mhaName, ip);
        return insertMachine(ip, port, uuid, BooleanEnum.FALSE.getCode(), mhaId);
    }

    private int insertMachine(String ip, int port, String uuid, int master, long mhaId) throws Exception {
        MachineTbl daoPojo = new MachineTbl();
        daoPojo.setIp(ip);
        daoPojo.setPort(port);
        daoPojo.setMaster(master);
        daoPojo.setUuid(uuid);
        daoPojo.setMhaId(mhaId);

        KeyHolder keyHolder = new KeyHolder();
        return machineTblDao.insert(new DalHints(), keyHolder, daoPojo);
    }

    @Override
    public List<String> getMhaMessengers(String mhaName) {
        try {
            MhaTblV2 mhaTblV2 = mhaTblV2Dao.queryByMhaName(mhaName);
            if (mhaTblV2 == null) {
                logger.info("mha: {} not exist", mhaName);
                return Collections.emptyList();
            }
            MessengerGroupTbl messengerGroupTbl = messengerGroupTblDao.queryByMhaId(mhaTblV2.getId(), BooleanEnum.FALSE.getCode());
            List<MessengerTbl> messengerTbls = messengerTblDao.queryByGroupId(messengerGroupTbl.getId());

            List<Long> resourceIds = messengerTbls.stream().map(MessengerTbl::getResourceId).collect(Collectors.toList());
            List<ResourceTbl> resourceTbl = resourceTblDao.queryByIds(resourceIds);
            List<String> ips = resourceTbl.stream().map(ResourceTbl::getIp).collect(Collectors.toList());
            return ips;
        } catch (SQLException e) {
            logger.error("getMhaAvailableMessengerResource exception", e);
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
    }

    @Override
    public DrcBuildPreCheckVo preCheckBeReplicatorIps(String mhaName, List<String> replicatorIps) throws Exception {
        MhaTblV2 srcMhaTbl = mhaTblV2Dao.queryByMhaName(mhaName, BooleanEnum.FALSE.getCode());
        if (srcMhaTbl == null) {
            return new DrcBuildPreCheckVo(null, null, DrcBuildPreCheckVo.NO_CONFLICT);
        }
        List<String> resourcesInUse = this.getMhaReplicators(mhaName);
        if (!resourcesCompare(resourcesInUse, replicatorIps)) {
            logger.info("[preCheck before build] try to update one2many share replicators,mha is {}", mhaName);
            return new DrcBuildPreCheckVo(mhaName, resourcesInUse, DrcBuildPreCheckVo.CONFLICT);
        }

        return new DrcBuildPreCheckVo(null, null, DrcBuildPreCheckVo.NO_CONFLICT);
    }

    private boolean resourcesCompare(List<String> resourcesInUse, List<String> replicatorsToBeUpdated) {
        if (resourcesInUse == null) return replicatorsToBeUpdated == null;
        else if (replicatorsToBeUpdated == null) return false;
        else if (resourcesInUse.size() != replicatorsToBeUpdated.size()) return false;
        else {
            List<String> copyResourcesInUse = Lists.newArrayList(resourcesInUse);
            copyResourcesInUse.removeAll(replicatorsToBeUpdated);
            return copyResourcesInUse.size() == 0;
        }
    }

    @Override
    public void updateMhaTag(String mhaName, String tag) throws Exception {
        MhaTblV2 mhaTblV2 = mhaTblV2Dao.queryByMhaName(mhaName);
        mhaTblV2.setTag(tag);
        mhaTblV2Dao.update(mhaTblV2);
    }

    @Override
    public String getMhaDc(String mhaName) throws Exception {
        MhaTblV2 mhaTblV2 = mhaTblV2Dao.queryByMhaName(mhaName, BooleanEnum.FALSE.getCode());
        if (mhaTblV2 == null) {
            return null;
        }
        DcTbl dcTbl = dcTblDao.queryById(mhaTblV2.getDcId());
        return dcTbl.getDcName();
    }

    @Override
    public String getRegion(String mhaName) throws SQLException {
        MhaTblV2 mhaTblV2 = mhaTblV2Dao.queryByMhaName(mhaName, BooleanEnum.FALSE.getCode());
        if (mhaTblV2 == null) {
            return null;
        }
        DcTbl dcTbl = dcTblDao.queryById(mhaTblV2.getDcId());
        return dcTbl.getRegionName();
    }


    @Override
    public Map<String, Long> getMhaReplicatorSlaveDelay(List<String> mhas) throws Exception {
        String trafficFromHickWall = domainConfig.getTrafficFromHickWall();
        String opsAccessToken = domainConfig.getOpsAccessToken();
        if (EnvUtils.fat()) {
            trafficFromHickWall = domainConfig.getTrafficFromHickWallFat();
            opsAccessToken = domainConfig.getOpsAccessTokenFat();
        }
        List<HickWallMhaReplicationDelayEntity> mhaReplicationDelay = opsApiServiceImpl.getMhaReplicationDelay(
                trafficFromHickWall, opsAccessToken);
        return mhaReplicationDelay.stream().filter(entity -> mhas.contains(entity.getSrcMha()))
                .collect(Collectors.toMap(
                        HickWallMhaReplicationDelayEntity::getSrcMha, HickWallMhaReplicationDelayEntity::getDelay,(e1, e2) -> e1));
    }

    @Override
    public List<String> queryMhasWithOutDrc() {
        List<String> mhas = Lists.newArrayList();
        Drc drc = metaProviderV2.getDrc();
        for (Entry<String, Dc> dcEntry : drc.getDcs().entrySet()) {
            Dc dc = dcEntry.getValue();
            for (Entry<String, DbCluster> dbClusterEntry : dc.getDbClusters().entrySet()) {
                DbCluster dbCluster = dbClusterEntry.getValue();
                List<Replicator> replicators = dbCluster.getReplicators();
                List<Applier> appliers = dbCluster.getAppliers();
                List<Messenger> messengers = dbCluster.getMessengers();
                if (replicators.isEmpty() && appliers.isEmpty() && messengers.isEmpty()) {
                    mhas.add(dbCluster.getMhaName());
                }
            }
        }
        return mhas;
    }

    @Override
    public Pair<Boolean,Integer> offlineMhasWithOutDrc(List<String> mhas) throws SQLException {
        List<String> mhaWithOutDrc = queryMhasWithOutDrc();
        mhas.retainAll(mhaWithOutDrc);
        if (CollectionUtils.isEmpty(mhas)) {
            return new Pair<>(Boolean.FALSE,0);
        }
        Pair<Boolean, Integer> res = Pair.of(Boolean.TRUE, 0);
        for (String mha : mhas) {
            if (offlineMha(mha)) {
                res.setValue(res.getValue() + 1);
            } else {
                logger.error("offline mha: {} failed", mha);
                res.setKey(Boolean.FALSE);
            }
        }
        return res;
    }

    // should check no use first
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public boolean offlineMha(String mhaName) throws SQLException {
        MhaTblV2 mhaTblV2 = mhaTblV2Dao.queryByMhaName(mhaName);
        if (mhaTblV2 == null) {
            logger.info("mha: {} not exist", mhaName);
            return false;
        }
        mhaTblV2.setDeleted(BooleanEnum.TRUE.getCode());
        List<MachineTbl> machineTbls = machineTblDao.queryByMhaId(mhaTblV2.getId(), BooleanEnum.FALSE.getCode());
        if (!CollectionUtils.isEmpty(machineTbls)) {
            for (MachineTbl machineTbl : machineTbls) {
                machineTbl.setDeleted(BooleanEnum.TRUE.getCode());
            }
        }
        machineTblDao.batchUpdate(machineTbls);
        int update = mhaTblV2Dao.update(mhaTblV2);
        return update == 1;
    }
    
}
