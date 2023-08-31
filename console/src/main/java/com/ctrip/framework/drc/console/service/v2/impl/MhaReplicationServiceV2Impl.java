package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.dao.DbTblDao;
import com.ctrip.framework.drc.console.dao.entity.DbTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.DbReplicationTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaDbMappingTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaReplicationTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.DbReplicationTblDao;
import com.ctrip.framework.drc.console.dao.v2.MhaDbMappingTblDao;
import com.ctrip.framework.drc.console.dao.v2.MhaReplicationTblDao;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
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
import com.ctrip.framework.drc.console.utils.StreamUtils;
import com.ctrip.framework.drc.core.http.PageResult;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    private final ExecutorService executorService = ThreadUtils.newFixedThreadPool(5, "mhaReplicationService");

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
}
