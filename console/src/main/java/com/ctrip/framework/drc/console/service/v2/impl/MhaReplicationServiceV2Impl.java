package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.dao.DbTblDao;
import com.ctrip.framework.drc.console.dao.MessengerGroupTblDao;
import com.ctrip.framework.drc.console.dao.ReplicatorGroupTblDao;
import com.ctrip.framework.drc.console.dao.ReplicatorTblDao;
import com.ctrip.framework.drc.console.dao.entity.DbTbl;
import com.ctrip.framework.drc.console.dao.entity.MessengerGroupTbl;
import com.ctrip.framework.drc.console.dao.entity.ReplicatorGroupTbl;
import com.ctrip.framework.drc.console.dao.entity.ReplicatorTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.*;
import com.ctrip.framework.drc.console.dao.entity.v3.ApplierGroupTblV3;
import com.ctrip.framework.drc.console.dao.entity.v3.ApplierTblV3;
import com.ctrip.framework.drc.console.dao.entity.v3.MhaDbReplicationTbl;
import com.ctrip.framework.drc.console.dao.v2.*;
import com.ctrip.framework.drc.console.dao.v3.ApplierGroupTblV3Dao;
import com.ctrip.framework.drc.console.dao.v3.ApplierTblV3Dao;
import com.ctrip.framework.drc.console.dao.v3.MhaDbReplicationTblDao;
import com.ctrip.framework.drc.console.dto.v2.MhaDelayInfoDto;
import com.ctrip.framework.drc.console.dto.v2.MhaDto;
import com.ctrip.framework.drc.console.dto.v2.MhaReplicationDto;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.ReadableErrorDefEnum;
import com.ctrip.framework.drc.console.enums.ReplicationTypeEnum;
import com.ctrip.framework.drc.console.param.v2.MhaReplicationQuery;
import com.ctrip.framework.drc.console.service.v2.MhaReplicationServiceV2;
import com.ctrip.framework.drc.console.service.v2.MysqlServiceV2;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.console.utils.MultiKey;
import com.ctrip.framework.drc.console.utils.StreamUtils;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.http.PageResult;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.platform.dal.dao.annotation.DalTransactional;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by dengquanliang
 * 2023/5/25 14:09
 */
@Service
public class MhaReplicationServiceV2Impl implements MhaReplicationServiceV2 {

    public static final int MAX_LOOP_COUNT = 20;
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final ExecutorService executorService = ThreadUtils.newCachedThreadPool("mhaReplicationService");

    @Autowired
    private MysqlServiceV2 mysqlServiceV2;
    @Autowired
    private MhaReplicationTblDao mhaReplicationTblDao;
    @Autowired
    private DbReplicationTblDao dbReplicationTblDao;
    @Autowired
    private DbTblDao dbTblDao;
    @Autowired
    private MhaDbMappingTblDao mhaDbMappingTblDao;
    @Autowired
    private MhaTblV2Dao mhaTblV2Dao;
    @Autowired
    private ApplierGroupTblV2Dao applierGroupTblV2Dao;
    @Autowired
    private ApplierTblV2Dao applierTblV2Dao;
    @Autowired
    private ReplicatorGroupTblDao replicatorGroupTblDao;
    @Autowired
    private ReplicatorTblDao replicatorTblDao;
    @Autowired
    private MessengerGroupTblDao messengerGroupTblDao;
    @Autowired
    private ApplierTblV3Dao applierTblV3Dao;
    @Autowired
    private ApplierGroupTblV3Dao applierGroupTblV3Dao;
    @Autowired
    private MhaDbReplicationTblDao mhaDbReplicationTblDao;


    @Override
    public PageResult<MhaReplicationTbl> queryByPage(MhaReplicationQuery query) {
        try {
            List<MhaReplicationTbl> data = mhaReplicationTblDao.queryByPage(query);
            int count = mhaReplicationTblDao.count(query);
            return PageResult.newInstance(data, query.getPageIndex(), query.getPageSize(), count);
        } catch (SQLException e) {
            logger.error("queryByPage error", e);
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
    }

    @Override
    public List<MhaReplicationTbl> queryRelatedReplications(List<Long> relatedMhaId) {
        try {
            return mhaReplicationTblDao.queryByRelatedMhaId(relatedMhaId);
        } catch (SQLException e) {
            logger.error("queryRelatedReplications error", e);
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
    }

    private List<MhaReplicationTbl> queryRelatedReplications(List<Long> relatedMhaId, boolean queryAll) {
        try {

            Set<Long> seenReplicationIds = Sets.newHashSet();
            Set<Long> seenMhaIds = Sets.newHashSet();
            List<MhaReplicationTbl> res = Lists.newArrayList();
            Set<Long> newMhaIds = Sets.newHashSet(relatedMhaId);
            int queryCount = 0;
            do {
                List<MhaReplicationTbl> newTbls = mhaReplicationTblDao.queryByRelatedMhaId(Lists.newArrayList(newMhaIds)).stream()
                        .filter(e -> !seenReplicationIds.contains(e.getId()))
                        .collect(Collectors.toList());
                res.addAll(newTbls);

                // update cache
                seenReplicationIds.addAll(newTbls.stream().map(MhaReplicationTbl::getId).collect(Collectors.toSet()));
                newMhaIds = newTbls.stream()
                        .flatMap(e -> Stream.of(e.getSrcMhaId(), e.getDstMhaId()))
                        .filter(e -> !seenMhaIds.contains(e))
                        .collect(Collectors.toSet());
                seenMhaIds.addAll(newMhaIds);
                // MAX_LOOP_COUNT prevent infinite loop
            } while (queryAll && !newMhaIds.isEmpty() && queryCount++ < MAX_LOOP_COUNT);

            return res;
        } catch (SQLException e) {
            logger.error("queryRelatedReplications error", e);
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
    }

    @Override
    public List<MhaReplicationTbl> queryRelatedReplicationByName(List<String> mhaNames, boolean queryAll) {
        try {
            List<MhaTblV2> mhaTblV2List = mhaTblV2Dao.queryByMhaNames(mhaNames, 0);
            List<Long> mhaIds = mhaTblV2List.stream().map(MhaTblV2::getId).collect(Collectors.toList());
            return queryRelatedReplications(mhaIds, queryAll);
        } catch (SQLException e) {
            logger.error("queryRelatedReplications error", e);
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
    }

    @Override
    public List<MhaReplicationDto> queryRelatedReplications(List<String> mhaNames, List<String> dbNames) {
        try {
            //oldMha + dbs -> related dbTbls, dbMhaMapping
            List<MhaTblV2> mhaTblV2 = mhaTblV2Dao.queryByMhaNames(mhaNames, BooleanEnum.FALSE.getCode());
            List<DbTbl> dbTbls = dbTblDao.queryByDbNames(dbNames);
            if (CollectionUtils.isEmpty(mhaTblV2) || CollectionUtils.isEmpty(dbTbls)) {
                return Collections.emptyList();
            }
            List<Long> dbIds = dbTbls.stream().map(DbTbl::getId).collect(Collectors.toList());
            List<Long> mhaIds = mhaTblV2.stream().map(MhaTblV2::getId).collect(Collectors.toList());
            List<MhaDbMappingTbl> mhaDbMappingTbls = mhaDbMappingTblDao.queryByDbIdsAndMhaIds(dbIds, mhaIds);
            if (CollectionUtils.isEmpty(mhaDbMappingTbls)) {
                return Collections.emptyList();
            }

            //dbReplications, mhaReplications
            List<Long> mappingIds = mhaDbMappingTbls.stream().map(MhaDbMappingTbl::getId).collect(Collectors.toList());
            List<DbReplicationTbl> relatedDbReplications = dbReplicationTblDao.queryByRelatedMappingIds(mappingIds, ReplicationTypeEnum.DB_TO_DB.getType())
                    .stream().filter(StreamUtils.distinctByKey(p -> p.getSrcMhaDbMappingId() + "," + p.getDstMhaDbMappingId())).collect(Collectors.toList());

            List<MhaReplicationDto> replicationDtoList = Lists.newArrayList();
            replicationDtoList.addAll(this.queryMhaReplicationFromDbReplication(relatedDbReplications));

            // build mha dto
            List<MhaDto> mhaDtoList = Lists.newArrayList();
            mhaDtoList.addAll(replicationDtoList.stream().map(MhaReplicationDto::getSrcMha).collect(Collectors.toList()));
            mhaDtoList.addAll(replicationDtoList.stream().map(MhaReplicationDto::getDstMha).collect(Collectors.toList()));

            List<Long> allMhaIds = mhaDtoList.stream().map(MhaDto::getId).collect(Collectors.toList());
            List<MhaTblV2> mhaTblV2List = mhaTblV2Dao.queryByIds(allMhaIds);
            Map<Long, MhaTblV2> mhaMap = mhaTblV2List.stream().collect(Collectors.toMap(MhaTblV2::getId, Function.identity()));
            mhaDtoList.forEach(e -> {
                MhaTblV2 tbl = mhaMap.get(e.getId());
                e.setName(tbl.getMhaName());
            });

            return replicationDtoList;
        } catch (SQLException e) {
            logger.error("queryRelatedReplications error", e);
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
    }

    @Override
    public List<MhaReplicationDto> queryReplicationByIds(List<Long> replicationIds) {
        try {
            List<MhaReplicationTbl> mhaReplicationTbls = mhaReplicationTblDao.queryByIds(replicationIds);
            Set<Long> mhaIds = Sets.newHashSet();
            mhaIds.addAll(mhaReplicationTbls.stream().map(MhaReplicationTbl::getSrcMhaId).collect(Collectors.toList()));
            mhaIds.addAll(mhaReplicationTbls.stream().map(MhaReplicationTbl::getDstMhaId).collect(Collectors.toList()));
            List<MhaTblV2> mhaTblV2List = mhaTblV2Dao.queryByIds(Lists.newArrayList(mhaIds));
            Map<Long, MhaTblV2> mhaMap = mhaTblV2List.stream().collect(Collectors.toMap(MhaTblV2::getId, e -> e));
            return mhaReplicationTbls.stream().map(e -> MhaReplicationDto.from(e, mhaMap)).collect(Collectors.toList());
        } catch (SQLException e) {
            logger.error("queryRelatedReplicationByIds error", e);
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
    }

    private List<MhaReplicationDto> queryMhaReplicationFromDbReplication(List<DbReplicationTbl> dbReplication) throws SQLException {
        if (CollectionUtils.isEmpty(dbReplication)) {
            return Collections.emptyList();
        }
        List<Long> allMappingIds = Lists.newArrayList();
        allMappingIds.addAll(dbReplication.stream().map(DbReplicationTbl::getSrcMhaDbMappingId).collect(Collectors.toList()));
        allMappingIds.addAll(dbReplication.stream().map(DbReplicationTbl::getDstMhaDbMappingId).collect(Collectors.toList()));
        allMappingIds = allMappingIds.stream().distinct().collect(Collectors.toList());

        List<MhaDbMappingTbl> mhaDbMappingTbls = mhaDbMappingTblDao.queryByIds(allMappingIds);
        List<Long> dbIds = mhaDbMappingTbls.stream().map(MhaDbMappingTbl::getDbId).collect(Collectors.toList());
        List<DbTbl> dbTbls = dbTblDao.queryByIds(dbIds);
        Map<Long, DbTbl> dbMap = dbTbls.stream().collect(Collectors.toMap(DbTbl::getId, Function.identity()));
        Map<Long, MhaDbMappingTbl> mappingTblMap = mhaDbMappingTbls.stream().collect(Collectors.toMap(MhaDbMappingTbl::getId, Function.identity()));
        Map<String, MhaReplicationDto> mhaReplicationMap = Maps.newHashMap();
        for (DbReplicationTbl dbReplicationTbl : dbReplication) {
            MhaDbMappingTbl srcMapping = mappingTblMap.get(dbReplicationTbl.getSrcMhaDbMappingId());
            MhaDbMappingTbl dstMapping = mappingTblMap.get(dbReplicationTbl.getDstMhaDbMappingId());
            // todo by yongnian: 2023/8/23 优化点
            String key = srcMapping.getMhaId() + "-" + dstMapping.getMhaId();
            MhaReplicationDto dto = mhaReplicationMap.get(key);
            if (dto == null) {
                MhaReplicationTbl mhaReplicationTbl = mhaReplicationTblDao.queryByMhaId(srcMapping.getMhaId(), dstMapping.getMhaId(), BooleanEnum.FALSE.getCode());
                dto = MhaReplicationDto.from(mhaReplicationTbl);
                mhaReplicationMap.put(key, dto);
            }
            DbTbl dbTbl = dbMap.get(srcMapping.getDbId());
            if (dbTbl != null) {
                dto.getDbs().add(dbTbl.getDbName());
            }
        }
        return Lists.newArrayList(mhaReplicationMap.values());
    }


    @Override
    public MhaDelayInfoDto getMhaReplicationDelay(String srcMha, String dstMha) {
        MhaDelayInfoDto delayInfoDto = new MhaDelayInfoDto();
        delayInfoDto.setDstMha(dstMha);
        delayInfoDto.setSrcMha(srcMha);

        // query dst first (result could be larger than querying src first)
        Long currentTime = mysqlServiceV2.getCurrentTime(srcMha);
        Long dstTime = mysqlServiceV2.getDelayUpdateTime(srcMha, dstMha);
        Long srcTime = mysqlServiceV2.getDelayUpdateTime(srcMha, srcMha);


        if (currentTime != null && srcTime != null) {
            srcTime = Math.max(srcTime, currentTime);
        } else {
            srcTime = null;
        }
        delayInfoDto.setSrcTime(srcTime);
        delayInfoDto.setDstTime(dstTime);
        return delayInfoDto;
    }


    @Override
    public List<MhaDelayInfoDto> getMhaReplicationDelays(List<MhaReplicationDto> mhaReplicationDtoList) {
        List<Callable<MhaDelayInfoDto>> list = Lists.newArrayList();
        for (MhaReplicationDto dto : mhaReplicationDtoList) {
            list.add(() -> this.getMhaReplicationDelay(dto.getSrcMha().getName(), dto.getDstMha().getName()));
        }

        try {
            List<MhaDelayInfoDto> res = Lists.newArrayList();
            List<Future<MhaDelayInfoDto>> futures = executorService.invokeAll(list, 5, TimeUnit.SECONDS);
            for (Future<MhaDelayInfoDto> future : futures) {
                res.add(future.get());
            }
            return res;
        } catch (InterruptedException | ExecutionException e) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_MHA_DELAY_FAIL, e);
        }
    }

    @Override
    public List<MhaDelayInfoDto> getMhaReplicationDelaysV2(List<MhaReplicationDto> mhaReplicationDtoList) {
        List<Callable<MhaDelayInfoDto>> list = Lists.newArrayList();
        for (MhaReplicationDto dto : mhaReplicationDtoList) {
            list.add(() -> this.getMhaReplicationDelay(dto.getSrcMha().getName(), dto.getDstMha().getName()));
        }

        try {
            List<MhaDelayInfoDto> res = Lists.newArrayList();
            List<Future<MhaDelayInfoDto>> futures = executorService.invokeAll(list, 5, TimeUnit.SECONDS);
            for (Future<MhaDelayInfoDto> future : futures) {
                if (!future.isCancelled()) {
                    res.add(future.get());
                }
            }
            return res;
        } catch (InterruptedException | ExecutionException e) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_MHA_DELAY_FAIL, e);
        }
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public boolean deleteMhaReplication(Long mhaReplicationId) throws SQLException {
        MhaReplicationTbl mhaReplicationTbl = mhaReplicationTblDao.queryById(mhaReplicationId);
        if (mhaReplicationTbl == null) return false;
        MhaTblV2 srcMha = mhaTblV2Dao.queryById(mhaReplicationTbl.getSrcMhaId());
        MhaTblV2 dstMha = mhaTblV2Dao.queryById(mhaReplicationTbl.getDstMhaId());
        List<MhaDbMappingTbl> srcMhaDbMappings = mhaDbMappingTblDao.queryByMhaId(srcMha.getId());
        List<MhaDbMappingTbl> dstMhaDbMappings = mhaDbMappingTblDao.queryByMhaId(dstMha.getId());
        List<DbReplicationTbl> dbReplicationTbls = dbReplicationTblDao.queryByMappingIds(
                srcMhaDbMappings.stream().map(MhaDbMappingTbl::getId).collect(Collectors.toList()),
                dstMhaDbMappings.stream().map(MhaDbMappingTbl::getId).collect(Collectors.toList()),
                ReplicationTypeEnum.DB_TO_DB.getType());
        if (!CollectionUtils.isEmpty(dbReplicationTbls)) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.REQUEST_PARAM_INVALID, "DbReplications not empty!" );
        }
        ApplierGroupTblV2 applierGroupTblV2 = applierGroupTblV2Dao.queryByMhaReplicationId(mhaReplicationId, BooleanEnum.FALSE.getCode());
        List<ApplierTblV2> applierTblV2s = applierTblV2Dao.queryByApplierGroupId(applierGroupTblV2.getId(),BooleanEnum.FALSE.getCode());
        if (!CollectionUtils.isEmpty(applierTblV2s)) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.REQUEST_PARAM_INVALID,
                    srcMha.getMhaName() + "==>" + dstMha.getMhaName() + ":Applier not empty!" );
        }

        List<MhaTblV2> mhasToBeDelete = Lists.newArrayList();
        markMhaOfflineIfNeed(srcMha, mhasToBeDelete);
        markMhaOfflineIfNeed(dstMha, mhasToBeDelete);

        logger.info("Going to delete mha replication, srcMha:{}, dstMha:{}", srcMha.getMhaName(), dstMha.getMhaName());
        mhaReplicationTbl.setDeleted(BooleanEnum.TRUE.getCode());
        mhaReplicationTblDao.update(mhaReplicationTbl);
        if (!mhasToBeDelete.isEmpty()) {
            mhaTblV2Dao.update(mhasToBeDelete);
        }
        return true;
    }

    private void markMhaOfflineIfNeed(MhaTblV2 mha, List<MhaTblV2> mhasToBeDelete) throws SQLException {
        MessengerGroupTbl messengerGroup = messengerGroupTblDao.queryByMhaId(mha.getId(), BooleanEnum.FALSE.getCode());
        if (messengerGroup != null) { // mha has messenger
            return;
        }
        ReplicatorGroupTbl mhaRGroup = replicatorGroupTblDao.queryByMhaId(mha.getId());
        List<ReplicatorTbl> mhaReplicators = replicatorTblDao.queryByRGroupIds(Lists.newArrayList(mhaRGroup.getId()), BooleanEnum.FALSE.getCode());
        List<MhaReplicationTbl> mhaReplications = mhaReplicationTblDao.queryByRelatedMhaId(Lists.newArrayList(mha.getId()));
        if (mhaReplications.size() == 1) { // going to mark mha as offline
            if (!CollectionUtils.isEmpty(mhaReplicators)) {
                throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.REQUEST_PARAM_INVALID,
                        mha.getMhaName() + "will offline,but Replicator not empty!");
            } else {
                logger.info("Going to mark mha {} as offline", mha.getMhaName());
                mha.setDeleted(BooleanEnum.TRUE.getCode());
                mhasToBeDelete.add(mha);
            }
        }
    }

    @Override
    public Map<String, String> parseConfigFileGtidContent(String configText) {
        String[] split = configText.split("\n");
        Map<String, String> map = new HashMap<>();
        for (String s : split) {
            if (s.startsWith("#")) {
                continue;
            }
            String[] keyAndValue = s.split("=");
            if (keyAndValue.length != 2) {
                continue;
            }
            String key = keyAndValue[0].trim();
            String value = keyAndValue[1].trim();
            if (StringUtils.isEmpty(key) || StringUtils.isEmpty(value)) {
                continue;
            }
            // check key
            if (!key.endsWith("purgedgtid") || key.split("\\.").length != 4) {
                continue;
            }
            // value check
            if (new GtidSet(value).getUUIDs().isEmpty()) {
                continue;
            }
            map.put(key, value);
        }
        return map;
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public Pair<Integer,List<String>> synApplierGtidInfoFromQConfig(String configText, boolean update) {
        try {
            Map<String, String> keyValueMap = this.parseConfigFileGtidContent(configText);

            Map<MhaReplicationDto, String> replicationToGtidMap = new HashMap<>();
            for (Map.Entry<String, String> entry : keyValueMap.entrySet()) {
                String[] split = entry.getKey().split("\\.");
                String srcMha = split[2];
                String dstMha = split[1];
                replicationToGtidMap.put(MhaReplicationDto.from(srcMha, dstMha), entry.getValue());
            }

            List<ApplierGroupTblV2> applierGroupUpdateList = Lists.newArrayList();
            List<String> illegalMessages = Lists.newArrayList();

            for (Map.Entry<MhaReplicationDto, String> entry : replicationToGtidMap.entrySet()) {
                MhaReplicationDto replicationDto = entry.getKey();
                String qConfigGtid = entry.getValue();
                String targetGtid = qConfigGtid;
                String srcName = replicationDto.getSrcMha().getName();
                String dstName = replicationDto.getDstMha().getName();
                String replication = srcName + "->" + dstName;

                MhaTblV2 srcMhaTbl = mhaTblV2Dao.queryByMhaName(srcName, BooleanEnum.FALSE.getCode());
                MhaTblV2 dstMhaTbl = mhaTblV2Dao.queryByMhaName(dstName, BooleanEnum.FALSE.getCode());
                if (srcMhaTbl == null || dstMhaTbl == null) {
                    illegalMessages.add(String.format("%s: mha not exit", replication));
                    continue;
                }
                MhaReplicationTbl mhaReplicationTbl = mhaReplicationTblDao.queryByMhaId(srcMhaTbl.getId(), dstMhaTbl.getId());
                if (mhaReplicationTbl == null) {
                    illegalMessages.add(String.format("%s: mha replication not exist", replication));
                    continue;
                }
                ApplierGroupTblV2 applierGroupTblV2 = applierGroupTblV2Dao.queryByMhaReplicationId(mhaReplicationTbl.getId(), BooleanEnum.FALSE.getCode());
                if (applierGroupTblV2 == null) {
                    illegalMessages.add(String.format("%s: applierGroup not exist", replication));
                    continue;
                }
                if (!StringUtils.isEmpty(applierGroupTblV2.getGtidInit())) {
                    targetGtid = new GtidSet(applierGroupTblV2.getGtidInit()).union(new GtidSet(qConfigGtid)).toString();
                    if (targetGtid.equals(applierGroupTblV2.getGtidInit())) {
                        continue;
                    }
                    illegalMessages.add(String.format("%s: going to update gitd. origin:%s, target:%S", replication, applierGroupTblV2.getGtidInit(), targetGtid));
                }
                logger.info("update gitd: {} {}->{} (qconfig gtid:{})", replication, applierGroupTblV2.getGtidInit(), targetGtid, qConfigGtid);
                applierGroupTblV2.setGtidInit(targetGtid);
                applierGroupUpdateList.add(applierGroupTblV2);
            }
            if (update) {
                int[] ints = applierGroupTblV2Dao.batchUpdate(applierGroupUpdateList);
                return Pair.of(Arrays.stream(ints).sum(), illegalMessages);
            }
            return Pair.of(applierGroupUpdateList.size(), illegalMessages);
        } catch (SQLException e) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION);
        }
    }

    @Override
    public List<MhaReplicationTbl> queryAllHasActiveMhaDbReplications(){
        try {
            List<ApplierTblV3> applierTblV3s = applierTblV3Dao.queryAllExist();
            List<ApplierGroupTblV3> applierGroupTblV3s = applierGroupTblV3Dao.queryAllExist();
            Set<Long> groupIds = applierTblV3s.stream().map(ApplierTblV3::getApplierGroupId).collect(Collectors.toSet());
            List<Long> mhaReplicationIds = applierGroupTblV3s.stream().filter(e -> groupIds.contains(e.getId())).map(ApplierGroupTblV3::getMhaDbReplicationId).collect(Collectors.toList());
            List<MhaDbReplicationTbl> mhaDbReplicationTbls = mhaDbReplicationTblDao.queryByIds(mhaReplicationIds);
            List<Long> mappingIds = mhaDbReplicationTbls.stream().flatMap(e -> Stream.of(e.getSrcMhaDbMappingId(), e.getDstMhaDbMappingId())).distinct().collect(Collectors.toList());
            List<MhaDbMappingTbl> mhaDbMappingTbls = mhaDbMappingTblDao.queryByIds(mappingIds);
            Map<Long, Long> map = mhaDbMappingTbls.stream().collect(Collectors.toMap(MhaDbMappingTbl::getId, MhaDbMappingTbl::getMhaId));
            // (src mha id , dst mha id)
            List<MhaReplicationTbl> samples = mhaDbReplicationTbls.stream().map(e -> new MultiKey(map.get(e.getSrcMhaDbMappingId()), map.get(e.getDstMhaDbMappingId())))
                    .distinct()
                    .map(e -> {
                        MhaReplicationTbl tbl = new MhaReplicationTbl();
                        tbl.setSrcMhaId((Long) e.getKey(0));
                        tbl.setDstMhaId((Long) e.getKey(1));
                        return tbl;
                    })
                    .collect(Collectors.toList());
            return mhaReplicationTblDao.queryBySamples(samples);
        } catch (SQLException e) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION);
        }
    }

}
