package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.dao.*;
import com.ctrip.framework.drc.console.dao.entity.*;
import com.ctrip.framework.drc.console.dao.entity.v2.*;
import com.ctrip.framework.drc.console.dao.entity.v3.*;
import com.ctrip.framework.drc.console.dao.v2.*;
import com.ctrip.framework.drc.console.dao.v3.*;
import com.ctrip.framework.drc.console.dto.MhaColumnDefaultValueDto;
import com.ctrip.framework.drc.console.dto.MhaColumnDefaultValueView;
import com.ctrip.framework.drc.console.dto.MhaInstanceGroupDto;
import com.ctrip.framework.drc.console.dto.v3.ReplicatorInfoDto;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.DrcAccountTypeEnum;
import com.ctrip.framework.drc.console.enums.ReadableErrorDefEnum;
import com.ctrip.framework.drc.console.enums.ReplicationTypeEnum;
import com.ctrip.framework.drc.console.exception.ConsoleException;
import com.ctrip.framework.drc.console.monitor.delay.config.v2.MetaProviderV2;
import com.ctrip.framework.drc.console.param.v2.MhaDbReplicationQuery;
import com.ctrip.framework.drc.console.param.v2.MhaQuery;
import com.ctrip.framework.drc.console.param.v2.MhaQueryParam;
import com.ctrip.framework.drc.console.param.v2.security.Account;
import com.ctrip.framework.drc.console.pojo.domain.DcDo;
import com.ctrip.framework.drc.console.service.impl.api.ApiContainer;
import com.ctrip.framework.drc.console.service.v2.MetaInfoServiceV2;
import com.ctrip.framework.drc.console.service.v2.MhaServiceV2;
import com.ctrip.framework.drc.console.service.v2.MysqlServiceV2;
import com.ctrip.framework.drc.console.service.v2.external.dba.DbaApiService;
import com.ctrip.framework.drc.console.service.v2.external.dba.response.ClusterInfoDto;
import com.ctrip.framework.drc.console.service.v2.security.AccountService;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.console.vo.check.DrcBuildPreCheckVo;
import com.ctrip.framework.drc.console.vo.request.MhaQueryDto;
import com.ctrip.framework.drc.core.entity.*;
import com.ctrip.framework.drc.core.monitor.enums.ModuleEnum;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.core.service.ops.OPSApiService;
import com.ctrip.framework.drc.core.service.statistics.traffic.HickWallMhaReplicationDelayEntity;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.KeyHolder;
import com.ctrip.platform.dal.dao.annotation.DalTransactional;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    @Autowired
    private MhaDbReplicationTblDao mhaDbReplicationTblDao;
    @Autowired
    private MhaReplicationTblDao mhaReplicationTblDao;
    @Autowired
    private ApplierGroupTblV3Dao applierGroupTblV3Dao;
    @Autowired
    private ApplierTblV3Dao applierTblV3Dao;
    @Autowired
    private MessengerGroupTblV3Dao messengerGroupTblV3Dao;
    @Autowired
    private MessengerTblV3Dao messengerTblV3Dao;
    @Autowired
    private DefaultConsoleConfig consoleConfig;
    @Autowired
    private AccountService accountService;
    @Autowired
    private MysqlServiceV2 mysqlServiceV2;

    private static final int COLUMN_DEFAULT_VALUE_MIN_LENGTH = 251;
    private static final int COLUMN_DEFAULT_VALUE_MAX_LENGTH = 255;

    private final ListeningExecutorService executeService = MoreExecutors.listeningDecorator(ThreadUtils.newCachedThreadPool("mhaExecuteService"));

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
    public List<ReplicatorInfoDto> getMhaReplicatorsV2(String mhaName) {
        try {
            MhaTblV2 mhaTblV2 = mhaTblV2Dao.queryByMhaName(mhaName);
            if (mhaTblV2 == null) {
                logger.info("replicatorGroupTbl not exist, mhaName: {}", mhaName);
                return Collections.emptyList();
            }

            ReplicatorGroupTbl replicatorGroupTbl = replicatorGroupTblDao.queryByMhaId(mhaTblV2.getId(), BooleanEnum.FALSE.getCode());
            if (replicatorGroupTbl == null) {
                logger.info("replicatorGroupTbl not exist, mhaName: {}", mhaName);
                return Collections.emptyList();
            }
            List<ReplicatorTbl> replicatorTbls = replicatorTblDao.queryByRGroupIds(Lists.newArrayList(replicatorGroupTbl.getId()), BooleanEnum.FALSE.getCode());
            if (CollectionUtils.isEmpty(replicatorTbls)) {
                return Collections.emptyList();
            }
            List<Long> resourceIds = replicatorTbls.stream().map(ReplicatorTbl::getResourceId).collect(Collectors.toList());
            List<ResourceTbl> resourceTbls = resourceTblDao.queryByIds(resourceIds);
            Map<Long, ResourceTbl> resourceIdMap = resourceTbls.stream().collect(Collectors.toMap(ResourceTbl::getId, Function.identity()));
            
            return replicatorTbls.stream().map(r -> new ReplicatorInfoDto(
                    r.getId(),
                    r.getGtidInit(),
                    r.getMaster().equals(BooleanEnum.TRUE.getCode()),
                    resourceIdMap.get(r.getResourceId()).getIp(),
                    resourceIdMap.get(r.getResourceId()).getTag(),
                    resourceIdMap.get(r.getResourceId()).getAz()
                    )
            ).collect(Collectors.toList());
            
        } catch (SQLException e) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
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
        Account account = accountService.getAccount(mhaTblV2, DrcAccountTypeEnum.DRC_CONSOLE);
        String uuid = MySqlUtils.getUuid(ip, port, account.getUser(), account.getPassword(), master);
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
        // todo hdpan get default new account from kms,no acc info in metaDB 
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
        // todo hdpan get default new account from kms,no acc info in metaDB
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
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public int updateMhaTag(List<String> mhaNames, String tag) throws Exception {
        int count = 0;
        for (String mhaName : mhaNames) {
            count += updateMhaTag(mhaName, tag);
        }
        return count;
    }

    @Override
    public int updateMhaTag(String mhaName, String tag) throws Exception {
        MhaTblV2 mhaTblV2 = mhaTblV2Dao.queryByMhaName(mhaName);
        mhaTblV2.setTag(tag);
        return mhaTblV2Dao.update(mhaTblV2);
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
        List<HickWallMhaReplicationDelayEntity> mhaReplicationDelay = opsApiServiceImpl.getMhaReplicationDelay(
                trafficFromHickWall, opsAccessToken);
        return mhaReplicationDelay.stream().filter(entity -> mhas.contains(entity.getSrcMha()))
                .collect(Collectors.toMap(
                        HickWallMhaReplicationDelayEntity::getSrcMha, HickWallMhaReplicationDelayEntity::getDelay, (e1, e2) -> e1));
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


    @Autowired
    private DbReplicationTblDao dbReplicationTblDao;

    @Override
    public Map<String, List<String>> getMhasWithoutDrcReplication(boolean checkDbReplication) {
        try {
            Drc drc = metaProviderV2.getRealtimeDrc();
            Map<String, List<String>> mhasWithoutDrcReplication = getMhasWithoutDrcReplication(drc);
            if (checkDbReplication) {
                Set<String> mhasWithoutDbReplicationConfig = getMhasWithoutDbReplicationConfigs();
                for (Entry<String, List<String>> entry : mhasWithoutDrcReplication.entrySet()) {
                    List<String> mhas = entry.getValue().stream().filter(mhasWithoutDbReplicationConfig::contains).collect(Collectors.toList());
                    entry.setValue(mhas);
                }
            }
            return mhasWithoutDrcReplication;
        } catch (SQLException e) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
    }

    @VisibleForTesting
    protected Set<String> getMhasWithoutDbReplicationConfigs() throws SQLException {
        List<DbReplicationTbl> dbReplicationTbls = dbReplicationTblDao.queryAllExist();
        Set<Long> configRelatedMappingIds = dbReplicationTbls.stream().flatMap(e -> {
            if (e.getReplicationType().equals(ReplicationTypeEnum.DB_TO_DB.getType())) {
                return Stream.of(e.getSrcMhaDbMappingId(), e.getDstMhaDbMappingId());
            } else if (e.getReplicationType().equals(ReplicationTypeEnum.DB_TO_MQ.getType())) {
                return Stream.of(e.getSrcMhaDbMappingId());
            } else {
                return Stream.empty();
            }
        }).collect(Collectors.toSet());

        List<MhaDbMappingTbl> mhaDbMappingTbls = mhaDbMappingTblDao.queryAllExist();
        Set<Long> configRelatedMhaIds = mhaDbMappingTbls.stream().filter(e -> configRelatedMappingIds.contains(e.getId()))
                .map(MhaDbMappingTbl::getMhaId).collect(Collectors.toSet());
        List<MhaTblV2> mhaTblV2List = mhaTblV2Dao.queryAllExist();
        return mhaTblV2List.stream().filter(e -> !configRelatedMhaIds.contains(e.getId())).map(MhaTblV2::getMhaName).collect(Collectors.toSet());
    }

    @VisibleForTesting
    protected Map<String, List<String>> getMhasWithoutDrcReplication(Drc drc) {
        List<DbCluster> dbClusters = drc.getDcs().values().stream().flatMap(e -> e.getDbClusters().values().stream()).collect(Collectors.toList());
        Set<String> drcRelatedMha = Sets.newHashSet();
        //
        for (DbCluster dbCluster : dbClusters) {
            if (CollectionUtils.isEmpty(dbCluster.getAppliers())
                    && CollectionUtils.isEmpty(dbCluster.getMessengers())) {
                continue;
            }

            String dstMha = dbCluster.getMhaName();
            Set<String> srcMhas = dbCluster.getAppliers().stream().map(Applier::getTargetMhaName).collect(Collectors.toSet());
            drcRelatedMha.add(dstMha);
            drcRelatedMha.addAll(srcMhas);
        }
        Map<String, List<String>> dcToMhaMap = new HashMap<>();
        for (Entry<String, Dc> dcEntry : drc.getDcs().entrySet()) {
            Dc dc = dcEntry.getValue();
            List<String> mhasWithReplicatorButNoDrc = dc.getDbClusters().values().stream()
                    .map(DbCluster::getMhaName)
                    .filter(mhaName -> !drcRelatedMha.contains(mhaName))
                    .collect(Collectors.toList());
            dcToMhaMap.computeIfAbsent(dc.getRegion(), (key) -> Lists.newArrayList())
                    .addAll(mhasWithReplicatorButNoDrc);
        }
        return dcToMhaMap;
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public Pair<Boolean, Integer> offlineMhasWithOutReplication(List<String> mhas) throws SQLException {
        if (CollectionUtils.isEmpty(mhas)) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.REQUEST_PARAM_INVALID, "empty input ");
        }
        List<String> allowDc = consoleConfig.getBatchOfflineRegion();
        if (CollectionUtils.isEmpty(allowDc)) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.REQUEST_PARAM_INVALID, "no allow dc found ");
        }
        Map<String, List<String>> mhasWithoutDrcReplication = getMhasWithoutDrcReplication(true);
        List<String> allowMhas = Lists.newArrayList();
        for (String dc : allowDc) {
            allowMhas.addAll(mhasWithoutDrcReplication.get(dc));
        }
        if (CollectionUtils.isEmpty(allowMhas)) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.REQUEST_PARAM_INVALID, "no allow mha found");
        }
        if (!Sets.newHashSet(allowMhas).containsAll(mhas)) {
            mhas.removeAll(allowMhas);
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.REQUEST_PARAM_INVALID, "not allowed mhas: " + mhas);
        }
        if (CollectionUtils.isEmpty(mhas)) {
            return new Pair<>(Boolean.FALSE, 0);
        }
        Pair<Boolean, Integer> res = Pair.of(Boolean.TRUE, 0);

        List<MhaTblV2> mhaTblV2List = mhaTblV2Dao.queryByMhaNames(mhas, BooleanEnum.FALSE.getCode());
        List<Long> mhaIds = mhaTblV2List.stream().map(MhaTblV2::getId).collect(Collectors.toList());
        // do offline 1: mha replication + applier group + messenger group
        this.offlineMhaReplications(mhaIds);

        // do offline 2: mha db replications + db applier group + db messenger group
        this.offLineMhaDbReplications(mhaIds);

        // do offline 3: mha replicators
        List<ReplicatorGroupTbl> replicatorGroupTbls = replicatorGroupTblDao.queryByMhaIds(mhaIds, BooleanEnum.FALSE.getCode());
        if (!CollectionUtils.isEmpty(replicatorGroupTbls)) {
            List<Long> replicatorGroupIds = replicatorGroupTbls.stream().map(ReplicatorGroupTbl::getId).collect(Collectors.toList());
            List<ReplicatorTbl> replicators = replicatorTblDao.queryByRGroupIds(replicatorGroupIds, BooleanEnum.FALSE.getCode());
            // update
            replicators.forEach(e -> e.setDeleted(BooleanEnum.TRUE.getCode()));
            replicatorTblDao.batchUpdate(replicators);
            replicatorGroupTbls.forEach(e -> e.setDeleted(BooleanEnum.TRUE.getCode()));
            replicatorGroupTblDao.batchUpdate(replicatorGroupTbls);
        }


        // do offline: mha
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

    private void offlineMhaReplications(List<Long> mhaIds) throws SQLException {
        if (CollectionUtils.isEmpty(mhaIds)) {
            return;
        }
        List<MhaReplicationTbl> mhaReplicationTblsToDelete = mhaReplicationTblDao.queryByRelatedMhaId(mhaIds);
        if (!CollectionUtils.isEmpty(mhaReplicationTblsToDelete)) {
            // update
            mhaReplicationTblsToDelete.forEach(e -> e.setDeleted(BooleanEnum.TRUE.getCode()));
            mhaReplicationTblDao.batchUpdate(mhaReplicationTblsToDelete);
        }
        // messenger
        List<MessengerGroupTbl> messengerGroupTblsToDelete = messengerGroupTblDao.queryByMhaIds(mhaIds, BooleanEnum.FALSE.getCode());
        if (!CollectionUtils.isEmpty(messengerGroupTblsToDelete)) {
            List<Long> messengerGroupTblIds = messengerGroupTblsToDelete.stream().map(MessengerGroupTbl::getId).collect(Collectors.toList());
            List<MessengerTbl> messengerTbls = messengerTblDao.queryByGroupIds(messengerGroupTblIds);
            if (!CollectionUtils.isEmpty(messengerTbls)) {
                List<Long> groupIds = messengerTbls.stream().map(MessengerTbl::getMessengerGroupId).distinct().collect(Collectors.toList());
                throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.REQUEST_PARAM_INVALID, "mha messenger still exist for group:  " + groupIds);
            }
            messengerGroupTblsToDelete.forEach(e -> e.setDeleted(BooleanEnum.TRUE.getCode()));
            messengerGroupTblDao.batchUpdate(messengerGroupTblsToDelete);
        }
    }

    private void offLineMhaDbReplications(List<Long> mhaIds) throws SQLException {
        if (CollectionUtils.isEmpty(mhaIds)) {
            return;
        }
        List<MhaDbMappingTbl> mhaDbMappingTblsToDelete = mhaDbMappingTblDao.queryByMhaIds(mhaIds);
        if (CollectionUtils.isEmpty(mhaDbMappingTblsToDelete)) {
            return;
        }
        List<Long> mappingIds = mhaDbMappingTblsToDelete.stream().map(MhaDbMappingTbl::getId).collect(Collectors.toList());
        MhaDbReplicationQuery query = new MhaDbReplicationQuery();
        query.setRelatedMappingList(mappingIds);
        List<MhaDbReplicationTbl> mhaDbReplicationTblsToDelete = mhaDbReplicationTblDao.query(query);

        // mha db applier
        List<Long> mhaDbReplicationApplierIds = mhaDbReplicationTblsToDelete.stream().filter(e -> ReplicationTypeEnum.DB_TO_DB.getType().equals(e.getReplicationType())).map(MhaDbReplicationTbl::getId).collect(Collectors.toList());
        List<ApplierGroupTblV3> applierGroupTblV3sToDelete = applierGroupTblV3Dao.queryByMhaDbReplicationIds(mhaDbReplicationApplierIds);
        List<Long> applierGroupV3Ids = applierGroupTblV3sToDelete.stream().map(ApplierGroupTblV3::getId).collect(Collectors.toList());
        List<ApplierTblV3> applierTblV3s = applierTblV3Dao.queryByApplierGroupIds(applierGroupV3Ids, BooleanEnum.FALSE.getCode());
        if (!CollectionUtils.isEmpty(applierTblV3s)) {
            List<Long> groupIds = applierTblV3s.stream().map(ApplierTblV3::getApplierGroupId).distinct().collect(Collectors.toList());
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.REQUEST_PARAM_INVALID, "mha db applier still exist for group:  " + groupIds);
        }
        // mha db messenger
        List<Long> mhaDbReplicationMessengerIds = mhaDbReplicationTblsToDelete.stream().filter(e -> ReplicationTypeEnum.DB_TO_MQ.getType().equals(e.getReplicationType())).map(MhaDbReplicationTbl::getId).collect(Collectors.toList());
        List<MessengerGroupTblV3> messengerGroupTblsV3ToDelete = messengerGroupTblV3Dao.queryByMhaDbReplicationIds(mhaDbReplicationMessengerIds);
        List<Long> messengerGroupTblV3Ids = messengerGroupTblsV3ToDelete.stream().map(MessengerGroupTblV3::getId).collect(Collectors.toList());
        List<MessengerTblV3> messengerTblsV3 = messengerTblV3Dao.queryByGroupIds(messengerGroupTblV3Ids);
        if (!CollectionUtils.isEmpty(messengerTblsV3)) {
            List<Long> groupIds = messengerTblsV3.stream().map(MessengerTblV3::getMessengerGroupId).distinct().collect(Collectors.toList());
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.REQUEST_PARAM_INVALID, "mha db messenger still exist for group:  " + groupIds);
        }

        // update
        mhaDbReplicationTblsToDelete.forEach(e -> e.setDeleted(BooleanEnum.TRUE.getCode()));
        mhaDbReplicationTblDao.batchUpdate(mhaDbReplicationTblsToDelete);
        applierGroupTblV3sToDelete.forEach(e -> e.setDeleted(BooleanEnum.TRUE.getCode()));
        applierGroupTblV3Dao.batchUpdate(applierGroupTblV3sToDelete);
        messengerGroupTblsV3ToDelete.forEach(e -> e.setDeleted(BooleanEnum.TRUE.getCode()));
        messengerGroupTblV3Dao.batchUpdate(messengerGroupTblsV3ToDelete);
    }

    @Override
    public Pair<Boolean, Integer> offlineMhasWithOutDrc(List<String> mhas) throws SQLException {
        List<String> mhaWithOutDrc = queryMhasWithOutDrc();
        mhas.retainAll(mhaWithOutDrc);
        if (CollectionUtils.isEmpty(mhas)) {
            return new Pair<>(Boolean.FALSE, 0);
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

    @Override
    public List<Long> queryMachineWithOutMha() throws SQLException {
        List<MhaTblV2> mhaTblV2s = mhaTblV2Dao.queryAllExist();
        List<MachineTbl> machineTbls = machineTblDao.queryAllExist();
        Set<Long> mhaIds = mhaTblV2s.stream().map(MhaTblV2::getId).collect(Collectors.toSet());
        return machineTbls.stream().filter(machineTbl -> !mhaIds.contains(machineTbl.getMhaId()))
                .map(MachineTbl::getId).collect(Collectors.toList());
    }

    @Override
    public Pair<Boolean, Integer> offlineMachineWithOutMha(List<Long> machineIds) throws SQLException {
        if (CollectionUtils.isEmpty(machineIds)) {
            return Pair.of(Boolean.FALSE, 0);
        }
        List<Long> machineIdCanOffline = this.queryMachineWithOutMha();
        machineIds.retainAll(machineIdCanOffline);
        if (CollectionUtils.isEmpty(machineIds)) {
            return Pair.of(Boolean.FALSE, 0);
        }
        Pair<Boolean, Integer> res = Pair.of(Boolean.TRUE, 0);
        machineTblDao.queryByPk(machineIds).forEach(machineTbl -> {
            machineTbl.setDeleted(BooleanEnum.TRUE.getCode());
            try {
                machineTblDao.update(machineTbl);
                res.setValue(res.getValue() + 1);
            } catch (SQLException e) {
                logger.error("offline machine: {} failed", machineTbl.getId(), e);
                res.setKey(Boolean.FALSE);
            }
        });
        return res;
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public boolean offlineMha(String mhaName) throws SQLException {
        MhaTblV2 mhaTblV2 = mhaTblV2Dao.queryByMhaName(mhaName);
        if (mhaTblV2 == null) {
            logger.info("mha: {} not exist", mhaName);
            return false;
        }
        mhaTblV2.setMonitorSwitch(BooleanEnum.FALSE.getCode());
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

    @Override
    public List<MhaTblV2> queryMhas(MhaQueryParam param) throws Exception {
        if (StringUtils.isNotBlank(param.getDbName())) {
            List<MhaTblV2> mhaTblV2s = queryRelatedMhaByDbName(Lists.newArrayList(param.getDbName()));
            if (CollectionUtils.isEmpty(mhaTblV2s)) {
                return new ArrayList<>();
            }
            param.setMhaIds(mhaTblV2s.stream().map(MhaTblV2::getId).collect(Collectors.toList()));
        }
        if (StringUtils.isNotBlank(param.getRegionName())) {
            List<DcTbl> dcTbls = dcTblDao.queryByRegionName(param.getRegionName());
            param.setDcIds(dcTbls.stream().map(DcTbl::getId).collect(Collectors.toList()));
        }

        return mhaTblV2Dao.queryByParam(param);
    }

    @Override
    public MachineTbl getMasterNode(Long mhaId) throws SQLException {
        List<MachineTbl> machineTbls = machineTblDao.queryByMhaId(mhaId, BooleanEnum.FALSE.getCode());
        if (CollectionUtils.isEmpty(machineTbls)) {
            return null;
        }
        return machineTbls.stream().filter(machineTbl -> machineTbl.getMaster().equals(BooleanEnum.TRUE.getCode()))
                .findFirst().orElse(null);
    }

    @Override
    public MhaColumnDefaultValueDto findColumnDefaultValueLengthGt251(String mha) {
        List<MySqlUtils.ColumnDefault> columnDefaultList = mysqlServiceV2.getMhaColumnDefaultValue(mha);
        if (CollectionUtils.isEmpty(columnDefaultList)) {
            return new MhaColumnDefaultValueDto(mha);
        }

        List<MySqlUtils.ColumnDefault> columnDefaults= columnDefaultList.stream().filter(e -> filterLength(e.getColumnDefault())).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(columnDefaults)) {
            return null;
        }

        Map<String, List<MySqlUtils.ColumnDefault>> columnDefaultMap = columnDefaults.stream().collect(Collectors.groupingBy(MySqlUtils.ColumnDefault::getFullTableName));

        List<MhaColumnDefaultValueDto.TableColumnDto> tableColumns = columnDefaultMap.entrySet().stream().map(entry -> {
            List<MhaColumnDefaultValueDto.ColumnDto> columns = entry.getValue().stream().map(columnDefault -> {
                MhaColumnDefaultValueDto.ColumnDto columnDto = new MhaColumnDefaultValueDto.ColumnDto(columnDefault.getColumnName(), columnDefault.getColumnDefault(), StringUtils.length(columnDefault.getColumnDefault()));
                return columnDto;
            }).collect(Collectors.toList());

            MhaColumnDefaultValueDto.TableColumnDto tableColumnDto = new MhaColumnDefaultValueDto.TableColumnDto(entry.getKey(), columns);
            return tableColumnDto;
        }).collect(Collectors.toList());

        return new MhaColumnDefaultValueDto(mha, tableColumns);
    }

    private boolean filterLength(String value) {
        return StringUtils.length(value) >= COLUMN_DEFAULT_VALUE_MIN_LENGTH && StringUtils.length(value) <= COLUMN_DEFAULT_VALUE_MAX_LENGTH;
    }

    @Override
    public MhaColumnDefaultValueView findColumnDefaultValueLengthGt251(List<String> mhas) {
        List<Callable<MhaColumnDefaultValueDto>> list = Lists.newArrayList();
        List<MhaColumnDefaultValueDto> results = Lists.newArrayList();
        List<String> errorMhas = Lists.newArrayList();
        for (String mha : mhas) {
            list.add(() -> findColumnDefaultValueLengthGt251(mha));
        }
        try {
            List<Future<MhaColumnDefaultValueDto>> futures = executeService.invokeAll(list, 60, TimeUnit.SECONDS);
            for (Future<MhaColumnDefaultValueDto> future : futures) {
                MhaColumnDefaultValueDto result = future.get();
                if (result != null) {
                    if (result.isQuerySuccess()) {
                        results.add(result);
                    } else {
                        errorMhas.add(result.getMhaName());
                    }

                }
            }
        } catch (InterruptedException | ExecutionException e) {
            throw ConsoleExceptionUtils.message("findColumnDefaultValueLengthGt251 timeout");
        }
        return new MhaColumnDefaultValueView(results, errorMhas);
    }

    @Override
    public MhaColumnDefaultValueView findAllColumnDefaultValueLengthGt251(int batch) throws SQLException {
        List<MhaColumnDefaultValueDto> results = Lists.newArrayList();
        List<String> errorMhas = Lists.newArrayList();
        List<String> mhas = mhaTblV2Dao.queryAllExist().stream().map(MhaTblV2::getMhaName).collect(Collectors.toList());
        if (batch == 0) {
            return findColumnDefaultValueLengthGt251(mhas);
        }

        List<List<String>> mhaPartitions = Lists.partition(mhas, batch);
        List<Callable<MhaColumnDefaultValueView>> list = Lists.newArrayList();
        for (List<String> partition : mhaPartitions) {
            list.add(() -> findColumnDefaultValueLengthGt251(partition));
        }

        try {
            List<Future<MhaColumnDefaultValueView>> futures = executeService.invokeAll(list, 60, TimeUnit.SECONDS);
            for (Future<MhaColumnDefaultValueView> future : futures) {
                MhaColumnDefaultValueView result = future.get();
                results.addAll(result.getMhaColumns());
                errorMhas.addAll(result.getErrorMhas());
            }
        } catch (InterruptedException | ExecutionException e) {
            throw ConsoleExceptionUtils.message("findColumnDefaultValueLengthGt251 timeout");
        }
        return new MhaColumnDefaultValueView(results, errorMhas);

    }
}
