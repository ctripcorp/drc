package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.*;
import com.ctrip.framework.drc.console.dao.entity.*;
import com.ctrip.framework.drc.console.dao.entity.v2.*;
import com.ctrip.framework.drc.console.dao.entity.v3.ApplierGroupTblV3;
import com.ctrip.framework.drc.console.dao.entity.v3.ApplierTblV3;
import com.ctrip.framework.drc.console.dao.v2.*;
import com.ctrip.framework.drc.console.dao.v3.ApplierGroupTblV3Dao;
import com.ctrip.framework.drc.console.dao.v3.ApplierTblV3Dao;
import com.ctrip.framework.drc.console.dto.v2.DbReplicationDto;
import com.ctrip.framework.drc.console.dto.v3.MhaDbDto;
import com.ctrip.framework.drc.console.dto.v3.MhaDbReplicationDto;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.ReadableErrorDefEnum;
import com.ctrip.framework.drc.console.enums.ReplicationTypeEnum;
import com.ctrip.framework.drc.console.param.v2.DbQuery;
import com.ctrip.framework.drc.console.pojo.domain.DcDo;
import com.ctrip.framework.drc.console.service.v2.DataMediaServiceV2;
import com.ctrip.framework.drc.console.service.v2.MessengerServiceV2;
import com.ctrip.framework.drc.console.service.v2.MetaInfoServiceV2;
import com.ctrip.framework.drc.console.service.v2.MhaDbReplicationService;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.console.utils.convert.TableNameBuilder;
import com.ctrip.framework.drc.core.entity.*;
import com.ctrip.framework.drc.core.meta.DataMediaConfig;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.ctrip.framework.drc.console.config.ConsoleConfig.DEFAULT_REPLICATOR_APPLIER_PORT;

/**
 * Created by yongnian
 * 2023/7/26 14:09
 */
@Service
public class MetaInfoServiceV2Impl implements MetaInfoServiceV2 {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final Supplier<List<DcDo>> dcCache = Suppliers.memoizeWithExpiration(this::queryAllDc, 10, TimeUnit.SECONDS);
    private final Supplier<List<RegionTbl>> regionCache = Suppliers.memoizeWithExpiration(this::queryAllRegion, 10, TimeUnit.SECONDS);
    private final Supplier<List<BuTbl>> buCache = Suppliers.memoizeWithExpiration(this::queryAllBu, 10, TimeUnit.SECONDS);
    private final Supplier<List<String>> dbBuCodeCache = Suppliers.memoizeWithExpiration(() -> {
        try {
            return this.dbTblDao.queryAllExist().stream().map(DbTbl::getBuCode).distinct().collect(Collectors.toList());
        } catch (SQLException e) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
    }, 1, TimeUnit.MINUTES);

    @Autowired
    private DcTblDao dcTblDao;
    @Autowired
    private BuTblDao buTblDao;
    @Autowired
    private RegionTblDao regionTblDao;
    @Autowired
    private MhaReplicationTblDao mhaReplicationTblDao;
    @Autowired
    private MhaTblV2Dao mhaTblV2Dao;
    @Autowired
    private MachineTblDao machineTblDao;
    @Autowired
    private ReplicatorTblDao replicatorTblDao;
    @Autowired
    private ReplicatorGroupTblDao replicatorGroupTblDao;
    @Autowired
    private ResourceTblDao resourceTblDao;
    @Autowired
    private DataMediaServiceV2 dataMediaService;
    @Autowired
    private ApplierGroupTblV2Dao applierGroupTblV2Dao;
    @Autowired
    private ApplierTblV2Dao applierTblV2Dao;
    @Autowired
    private MhaDbMappingTblDao mhaDbMappingTblDao;
    @Autowired
    private DbTblDao dbTblDao;
    @Autowired
    private DbReplicationTblDao dbReplicationTblDao;
    @Autowired
    private MessengerServiceV2 messengerService;
    @Autowired
    private DefaultConsoleConfig consoleConfig;
    @Autowired
    private ApplierTblV3Dao applierTblV3Dao;
    @Autowired
    private ApplierGroupTblV3Dao applierGroupTblV3Dao;
    @Autowired
    private MhaDbReplicationService mhaDbReplicationService;


    @Override
    public Drc getDrcReplicationConfig(Long replicationId) {
        Drc drc = new Drc();
        try {
            MhaReplicationTbl replicationTbl = mhaReplicationTblDao.queryById(replicationId);
            if (replicationTbl == null) {
                throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_RESULT_EMPTY, "replication not exist: " + replicationId);
            }

            List<MhaTblV2> mhaTblV2List = mhaTblV2Dao.queryByIds(Lists.newArrayList(replicationTbl.getSrcMhaId(), replicationTbl.getDstMhaId()));
            MhaTblV2 srcMhaTbl = mhaTblV2List.stream().filter(e -> Objects.equals(e.getId(), replicationTbl.getSrcMhaId())).findFirst().orElseThrow(() -> ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_RESULT_EMPTY, "src mha not exist: " + replicationTbl.getSrcMhaId()));
            MhaTblV2 dstMhaTbl = mhaTblV2List.stream().filter(e -> Objects.equals(e.getId(), replicationTbl.getDstMhaId())).findFirst().orElseThrow(() -> ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_RESULT_EMPTY, "dst mha not exist: " + replicationTbl.getDstMhaId()));

            this.appendReplicationConfig(drc, srcMhaTbl, dstMhaTbl);
            this.appendReplicationConfig(drc, dstMhaTbl, srcMhaTbl);
        } catch (SQLException e) {
            logger.error("getDrcReplicationConfig sql exception", e);
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
        return drc;
    }

    @Override
    public Drc getDrcReplicationConfig(String srcMhaName, String dstMhaName) {
        Drc drc = new Drc();
        try {
            MhaTblV2 srcMhaTbl = mhaTblV2Dao.queryByMhaName(srcMhaName, 0);
            MhaTblV2 dstMhaTbl = mhaTblV2Dao.queryByMhaName(dstMhaName, 0);
            if (srcMhaTbl == null || dstMhaTbl == null) {
                throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.REQUEST_PARAM_INVALID, "mha not found!");
            }
            this.appendReplicationConfig(drc, srcMhaTbl, dstMhaTbl);
            this.appendReplicationConfig(drc, dstMhaTbl, srcMhaTbl);
        } catch (SQLException e) {
            logger.error("getDrcReplicationConfig sql exception", e);
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
        return drc;
    }

    @Override
    public Drc getDrcMessengerConfig(String mhaName) {
        Drc drc = new Drc();
        try {
            MhaTblV2 mhaTblV2 = mhaTblV2Dao.queryByMhaName(mhaName, 0);
            if (mhaTblV2 == null) {
                throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_RESULT_EMPTY, "replication not exist: " + mhaName);
            }
            this.appendMessengerConfig(drc, mhaTblV2);
        } catch (SQLException e) {
            logger.error("getDrcMessengerConfig sql exception", e);
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
        return drc;
    }

    private void appendMessengerConfig(Drc drc, MhaTblV2 mhaTbl) throws SQLException {
        DcDo dcDo = this.queryAllDc()
                .stream()
                .filter(e -> e.getDcId().equals(mhaTbl.getDcId()))
                .findFirst()
                .orElseThrow(() -> ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_RESULT_EMPTY, "dc not exist: " + mhaTbl.getDcId()));

        Dc dc = generateDcFrame(drc, dcDo);
        DbCluster dbCluster = generateDbCluster(dc, mhaTbl);
        generateDbs(dbCluster, mhaTbl);
        generateReplicators(dbCluster, mhaTbl);
        generateMessengers(dbCluster, mhaTbl);
    }


    private void appendReplicationConfig(Drc drc, MhaTblV2 mhaTbl, MhaTblV2 dstMhaTbl) throws SQLException {
        DcDo dcDo = this.queryAllDc().stream().filter(e -> e.getDcId().equals(mhaTbl.getDcId())).findFirst().orElseThrow(() -> ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_RESULT_EMPTY, "dc not exist: " + mhaTbl.getDcId()));

        Dc dc = generateDcFrame(drc, dcDo);
        DbCluster dbCluster = generateDbCluster(dc, mhaTbl);
        generateDbs(dbCluster, mhaTbl);
        generateReplicators(dbCluster, mhaTbl);
        generateAppliers(dbCluster, mhaTbl, dstMhaTbl);
    }


    private void generateAppliers(DbCluster dbCluster, MhaTblV2 mhaTbl, MhaTblV2 srcMhaTbl) throws SQLException {
        MhaReplicationTbl mhaReplicationTbl = mhaReplicationTblDao.queryByMhaId(srcMhaTbl.getId(), mhaTbl.getId(), 0);
        if (mhaReplicationTbl == null) {
            return;
        }
        ApplierGroupTblV2 applierGroupTbl = applierGroupTblV2Dao.queryByMhaReplicationId(mhaReplicationTbl.getId(), 0);
        if (applierGroupTbl == null) {
            return;
        }
        if (consoleConfig.getMetaGeneratorV5Switch()) {
            generateDbApplierInstances(dbCluster, srcMhaTbl, mhaTbl);
        }
        if (CollectionUtils.isEmpty(dbCluster.getAppliers())) {
            generateApplierInstances(dbCluster, srcMhaTbl, mhaTbl, applierGroupTbl);
        }
    }

    private void generateMessengers(DbCluster dbCluster, MhaTblV2 mhaTbl) throws SQLException {
        if (consoleConfig.getMetaGeneratorV5Switch()) {
            List<Messenger> messengers = messengerService.generateDbMessengers(mhaTbl.getId());
            messengers.forEach(dbCluster::addMessenger);
        }
        if (CollectionUtils.isEmpty(dbCluster.getMessengers())) {
            List<Messenger> messengers = messengerService.generateMessengers(mhaTbl.getId());
            messengers.forEach(dbCluster::addMessenger);
        }
    }

    private void generateDbApplierInstances(DbCluster dbCluster, MhaTblV2 srcMhaTbl, MhaTblV2 dstMhaTbl) throws SQLException {
        // mha db replications
        List<MhaDbReplicationDto> replicationDtos = mhaDbReplicationService.queryByMha(srcMhaTbl.getMhaName(), dstMhaTbl.getMhaName(), null);

        // db replications
        List<DbReplicationTbl> samples = replicationDtos.stream().map(e -> {
            DbReplicationTbl tbl = new DbReplicationTbl();
            tbl.setSrcMhaDbMappingId(e.getSrc().getMhaDbMappingId());
            tbl.setDstMhaDbMappingId(e.getDst().getMhaDbMappingId());
            tbl.setReplicationType(e.getReplicationType());
            return tbl;
        }).collect(Collectors.toList());
        List<DbReplicationTbl> allDbReplicationTblList = dbReplicationTblDao.queryBySamples(samples);
        Map<Pair<Long, Long>, List<DbReplicationTbl>> dbReplicationTblGroupingBy = allDbReplicationTblList.stream().collect(Collectors.groupingBy(e -> Pair.from(e.getSrcMhaDbMappingId(), e.getDstMhaDbMappingId())));
        // appliers
        List<ApplierGroupTblV3> applierGroupTblV3s = applierGroupTblV3Dao.queryByMhaDbReplicationIds(replicationDtos.stream().map(MhaDbReplicationDto::getId).collect(Collectors.toList()));
        Map<Long, ApplierGroupTblV3> groupMap = applierGroupTblV3s.stream().collect(Collectors.toMap(ApplierGroupTblV3::getMhaDbReplicationId, e -> e));
        List<ApplierTblV3> mhaAppliers = applierTblV3Dao.queryByApplierGroupIds(applierGroupTblV3s.stream().map(ApplierGroupTblV3::getId).collect(Collectors.toList()), 0);
        Map<Long, List<ApplierTblV3>> applierMapByGroupId = mhaAppliers.stream().collect(Collectors.groupingBy(e -> e.getApplierGroupId()));

        // prepare data
        // dc
        DcDo srcDcTbl = this.queryAllDcWithCache().stream()
                .filter(e -> e.getDcId().equals(srcMhaTbl.getDcId())).findFirst()
                .orElseThrow(() -> ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_DATA_INCOMPLETE, "dc not exist: " + srcMhaTbl.getDcId()));

        // resource ip
        List<Long> applierResourceIds = mhaAppliers.stream().map(ApplierTblV3::getResourceId).distinct().collect(Collectors.toList());
        Map<Long, ResourceTbl> resourceTblMap = resourceTblDao.queryByIds(applierResourceIds).stream().collect(Collectors.toMap(ResourceTbl::getId, e -> e));

        // mappingId -> dbName
        // build
        Map<Long, String> mhaDbMappingMap = replicationDtos.stream().flatMap(e -> Stream.of(e.getSrc(), e.getDst())).collect(Collectors.toMap(MhaDbDto::getMhaDbMappingId, MhaDbDto::getDbName));

        for (MhaDbReplicationDto replicationDto : replicationDtos) {
            ApplierGroupTblV3 groupTblV3 = groupMap.get(replicationDto.getId());
            if (groupTblV3 == null) {
                continue;
            }
            List<ApplierTblV3> dbAppliers = applierMapByGroupId.get(groupTblV3.getId());
            if (CollectionUtils.isEmpty(dbAppliers)) {
                continue;
            }
            List<DbReplicationTbl> dbReplicationTbls = dbReplicationTblGroupingBy.get(Pair.from(replicationDto.getSrc().getMhaDbMappingId(), replicationDto.getDst().getMhaDbMappingId()));
            if (CollectionUtils.isEmpty(dbReplicationTbls)) {
                continue;
            }
            for (ApplierTblV3 applierTbl : dbAppliers) {
                String resourceIp = Optional.ofNullable(resourceTblMap.get(applierTbl.getResourceId())).map(ResourceTbl::getIp).orElse(StringUtils.EMPTY);
                Applier applier = new Applier();
                applier.setIp(resourceIp)
                        .setPort(applierTbl.getPort())
                        .setTargetIdc(srcDcTbl.getDcName())
                        .setTargetRegion(srcDcTbl.getRegionName())
                        .setTargetMhaName(srcMhaTbl.getMhaName())
                        .setGtidExecuted(groupTblV3.getGtidInit())
                        .setIncludedDbs(replicationDto.getSrc().getDbName().toLowerCase())
                        .setNameFilter(TableNameBuilder.buildNameFilter(mhaDbMappingMap, dbReplicationTbls))
                        .setNameMapping(TableNameBuilder.buildNameMapping(mhaDbMappingMap, dbReplicationTbls))
                        .setTargetName(srcMhaTbl.getClusterName())
                        .setApplyMode(ApplyMode.db_transaction_table.getType())
                        .setProperties(getProperties(dbReplicationTbls, groupTblV3.getConcurrency()));
                dbCluster.addApplier(applier);
            }
        }
    }

    private void generateApplierInstances(DbCluster dbCluster, MhaTblV2 srcMhaTbl, MhaTblV2 dstMhaTbl, ApplierGroupTblV2 applierGroupTbl) throws SQLException {
        // db mappings
        List<MhaDbMappingTbl> mhaDbMappingTbls = mhaDbMappingTblDao.queryByMhaIds(Lists.newArrayList(srcMhaTbl.getId(), dstMhaTbl.getId()));
        Map<Long, List<MhaDbMappingTbl>> mhaDbMappingMap = mhaDbMappingTbls.stream().collect(Collectors.groupingBy(MhaDbMappingTbl::getMhaId));
        List<MhaDbMappingTbl> srcMhaDbMappingTbls = mhaDbMappingMap.getOrDefault(srcMhaTbl.getId(), new ArrayList<>());
        List<MhaDbMappingTbl> dstMhaDbMappingTbls = mhaDbMappingMap.getOrDefault(dstMhaTbl.getId(), new ArrayList<>());
        List<Long> srcMhaDbMappingIds = srcMhaDbMappingTbls.stream().map(MhaDbMappingTbl::getId).collect(Collectors.toList());
        List<Long> dstMhaDbMappingIds = dstMhaDbMappingTbls.stream().map(MhaDbMappingTbl::getId).collect(Collectors.toList());
        List<DbReplicationTbl> dbReplicationTblList = dbReplicationTblDao.queryByMappingIds(srcMhaDbMappingIds, dstMhaDbMappingIds, ReplicationTypeEnum.DB_TO_DB.getType());

        // mappingId -> dbId
        Map<Long, Long> srcMhaDbMappingMap = srcMhaDbMappingTbls.stream().collect(Collectors.toMap(MhaDbMappingTbl::getId, MhaDbMappingTbl::getDbId));
        Map<Long, Long> dstMhaDbMappingMap = dstMhaDbMappingTbls.stream().collect(Collectors.toMap(MhaDbMappingTbl::getId, MhaDbMappingTbl::getDbId));

        // dbTblMap
        List<Long> srcDbIds = srcMhaDbMappingTbls.stream().map(MhaDbMappingTbl::getDbId).collect(Collectors.toList());
        List<Long> dstDbIds = dstMhaDbMappingTbls.stream().map(MhaDbMappingTbl::getDbId).collect(Collectors.toList());
        List<Long> dbIds = Stream.concat(srcDbIds.stream(), dstDbIds.stream()).distinct().collect(Collectors.toList());
        List<DbTbl> dbTbls = dbTblDao.queryByIds(dbIds);
        Map<Long, String> srcDbTblMap = dbTbls.stream().filter(e -> srcDbIds.contains(e.getId())).collect(Collectors.toMap(DbTbl::getId, DbTbl::getDbName));
        Map<Long, String> dstDbTblMap = dbTbls.stream().filter(e -> dstDbIds.contains(e.getId())).collect(Collectors.toMap(DbTbl::getId, DbTbl::getDbName));

        // appliers
        List<ApplierTblV2> curMhaAppliers = applierTblV2Dao.queryByApplierGroupId(applierGroupTbl.getId(), 0);

        // dc
        DcDo srcDcTbl = this.queryAllDcWithCache().stream().filter(e -> e.getDcId().equals(srcMhaTbl.getDcId())).findFirst().orElseThrow(() -> ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_DATA_INCOMPLETE, "dc not exist: " + srcMhaTbl.getDcId()));

        // resource ip
        List<Long> applierResourceIds = curMhaAppliers.stream().map(ApplierTblV2::getResourceId).distinct().collect(Collectors.toList());
        Map<Long, ResourceTbl> resourceTblMap = resourceTblDao.queryByIds(applierResourceIds).stream().collect(Collectors.toMap(ResourceTbl::getId, e -> e));

        // build
        for (ApplierTblV2 applierTbl : curMhaAppliers) {
            String resourceIp = Optional.ofNullable(resourceTblMap.get(applierTbl.getResourceId())).map(ResourceTbl::getIp).orElse(StringUtils.EMPTY);
            Applier applier = new Applier();
            applier.setIp(resourceIp)
                    .setPort(applierTbl.getPort())
                    .setTargetIdc(srcDcTbl.getDcName())
                    .setTargetRegion(srcDcTbl.getRegionName())
                    .setTargetMhaName(srcMhaTbl.getMhaName())
                    .setGtidExecuted(applierGroupTbl.getGtidInit())
                    .setNameFilter(TableNameBuilder.buildNameFilter(srcDbTblMap, srcMhaDbMappingMap, dbReplicationTblList))
                    .setNameMapping(TableNameBuilder.buildNameMapping(srcDbTblMap, srcMhaDbMappingMap, dstDbTblMap, dstMhaDbMappingMap, dbReplicationTblList))
                    .setTargetName(srcMhaTbl.getClusterName())
                    .setApplyMode(dstMhaTbl.getApplyMode())
                    .setProperties(getProperties(dbReplicationTblList, null));
            dbCluster.addApplier(applier);
        }
    }

    private void generateReplicators(DbCluster dbCluster, MhaTblV2 mhaTbl) throws SQLException {
        ReplicatorGroupTbl replicatorGroupTbl = replicatorGroupTblDao.queryByMhaId(mhaTbl.getId());
        if (null == replicatorGroupTbl) {
            return;
        }
        List<ReplicatorTbl> curMhaReplicators = replicatorTblDao.queryByRGroupIds(Lists.newArrayList(replicatorGroupTbl.getId()), BooleanEnum.FALSE.getCode());
        if (CollectionUtils.isEmpty(curMhaReplicators)) {
            return;
        }

        List<Long> resourceIdList = curMhaReplicators.stream().map(ReplicatorTbl::getResourceId).distinct().collect(Collectors.toList());
        Map<Long, ResourceTbl> resoueceMap = resourceTblDao.queryByIds(resourceIdList).stream().collect(Collectors.toMap(ResourceTbl::getId, Function.identity()));
        for (ReplicatorTbl replicatorTbl : curMhaReplicators) {
            ResourceTbl resourceTbl = resoueceMap.get(replicatorTbl.getResourceId());
            Replicator replicator = new Replicator();
            replicator.setIp(resourceTbl.getIp())
                    .setPort(replicatorTbl.getPort())
                    .setApplierPort(replicatorTbl.getApplierPort())
                    .setExcludedTables(replicatorGroupTbl.getExcludedTables())
                    .setGtidSkip(replicatorTbl.getGtidInit())
                    .setMaster(BooleanEnum.TRUE.getCode().equals(replicatorTbl.getMaster()));

            dbCluster.addReplicator(replicator);
        }
    }

    private void generateDbs(DbCluster dbCluster, MhaTblV2 mhaTbl) throws SQLException {
        Dbs dbs = new Dbs();
        dbCluster.setDbs(dbs);

        List<MachineTbl> mhaMachineTblList = machineTblDao.queryByMhaId(mhaTbl.getId(), BooleanEnum.FALSE.getCode());
        for (MachineTbl machineTbl : mhaMachineTblList) {
            Db db = new Db();
            db.setIp(machineTbl.getIp())
                    .setPort(machineTbl.getPort())
                    .setMaster(machineTbl.getMaster().equals(BooleanEnum.TRUE.getCode()))
                    .setUuid(machineTbl.getUuid());
            dbs.addDb(db);
        }
    }

    private DbCluster generateDbCluster(Dc dc, MhaTblV2 mhaTbl) {
        String mhaName = mhaTbl.getMhaName();
        String buName = this.queryAllBuWithCache().stream().filter(e -> e.getId().equals(mhaTbl.getBuId())).findFirst().map(BuTbl::getBuName).orElse(StringUtils.EMPTY);

        DbCluster dbCluster = new DbCluster();
        dbCluster.setId(mhaTbl.getClusterName() + '.' + mhaName)
                .setName(mhaTbl.getClusterName())
                .setMhaName(mhaName)
                .setBuName(buName)
                .setAppId(mhaTbl.getAppId())
                .setOrgId(mhaTbl.getBuId().intValue())
                .setApplyMode(mhaTbl.getApplyMode());
        dc.addDbCluster(dbCluster);
        return dbCluster;
    }

    private Dc generateDcFrame(Drc drc, DcDo dcDo) {

        // dc
        Dc dc = drc.findDc(dcDo.getDcName());
        if (dc == null) {
            dc = new Dc(dcDo.getDcName());
            dc.setRegion(dcDo.getRegionName());
            drc.addDc(dc);
        }
        return dc;
    }

    private String getProperties(List<DbReplicationTbl> dbReplicationTblList, Integer concurrency) throws SQLException {
        List<DbReplicationDto> dbReplicationDto = dbReplicationTblList.stream().map(source -> {
            DbReplicationDto target = new DbReplicationDto();
            target.setDbReplicationId(source.getId());
            target.setSrcMhaDbMappingId(source.getSrcMhaDbMappingId());
            target.setSrcLogicTableName(source.getSrcLogicTableName());
            return target;
        }).collect(Collectors.toList());

        DataMediaConfig properties = dataMediaService.generateConfigFast(dbReplicationDto);
        properties.setConcurrency(concurrency);
        boolean emptyProperties = CollectionUtils.isEmpty(properties.getRowsFilters())
                && CollectionUtils.isEmpty(properties.getColumnsFilters())
                && properties.getConcurrency() == null;
        return emptyProperties ? null : JsonCodec.INSTANCE.encode(properties);
    }

    @Override
    public List<DcDo> queryAllDcWithCache() {
        return dcCache.get();
    }

    @Override
    public List<DbTbl> queryDbByPage(DbQuery dbQuery) {
        try {
            return dbTblDao.queryByPage(dbQuery);
        } catch (SQLException e) {
            logger.error("queryAllRegion exception", e);
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
    }

    @Override
    public List<RegionTbl> queryAllRegionWithCache() {
        return regionCache.get();
    }

    @Override
    public List<BuTbl> queryAllBuWithCache() {
        return buCache.get();
    }

    @Override
    public List<BuTbl> queryAllBu() {
        try {
            return buTblDao.queryAll();
        } catch (SQLException e) {
            logger.error("queryAllBu exception", e);
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
    }

    @Override
    public List<String> queryAllDbBuCode() {
        return dbBuCodeCache.get();
    }

    @Override
    public List<RegionTbl> queryAllRegion() {
        try {
            return regionTblDao.queryAll();
        } catch (SQLException e) {
            logger.error("queryAllRegion exception", e);
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
    }


    @Override
    public List<DcDo> queryAllDc() {
        try {
            logger.info("queryAllDc");
            List<DcTbl> dcTbls = dcTblDao.queryAll();
            List<RegionTbl> regionTbls = regionTblDao.queryAll();
            Map<String, RegionTbl> regionTblMap = regionTbls.stream().collect(Collectors.toMap(RegionTbl::getRegionName, Function.identity()));

            return dcTbls.stream().map(e -> {
                DcDo dcDo = new DcDo();
                dcDo.setDcId(e.getId());
                dcDo.setDcName(e.getDcName());
                dcDo.setRegionName(e.getRegionName());

                RegionTbl regionTbl = regionTblMap.get(e.getRegionName());
                if (regionTbl != null) {
                    dcDo.setRegionId(regionTbl.getId());
                }
                return dcDo;
            }).collect(Collectors.toList());
        } catch (SQLException e) {
            logger.error("queryAllRegion exception", e);
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
    }

    public Integer findAvailableApplierPort(String ip) throws SQLException {
        ResourceTbl resourceTbl = resourceTblDao.queryAll().stream().filter(predicate -> (predicate.getDeleted().equals(BooleanEnum.FALSE.getCode()) && predicate.getIp().equalsIgnoreCase(ip))).findFirst().get();
        List<ReplicatorTbl> replicatorTbls = replicatorTblDao.queryAll().stream().filter(r -> r.getDeleted().equals(BooleanEnum.FALSE.getCode()) && r.getResourceId().equals(resourceTbl.getId())).collect(Collectors.toList());
        if (replicatorTbls.size() == 0) {
            return DEFAULT_REPLICATOR_APPLIER_PORT;
        }
        int size = consoleConfig.getAvailablePortSize();
        boolean[] isUsedFlags = new boolean[size];
        for (ReplicatorTbl r : replicatorTbls) {
            int index = r.getApplierPort() - DEFAULT_REPLICATOR_APPLIER_PORT;
            isUsedFlags[index] = true;
        }
        for (int i = 0; i <= size; i++) {
            if (!isUsedFlags[i]) {
                return DEFAULT_REPLICATOR_APPLIER_PORT + i;
            }
        }
        throw ConsoleExceptionUtils.message("no available port find for replicator, all in use!");
    }
}
