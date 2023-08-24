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
import com.ctrip.framework.drc.console.dto.v2.MhaReplicationDto;
import com.ctrip.framework.drc.console.enums.ReadableErrorDefEnum;
import com.ctrip.framework.drc.console.enums.ReplicationTypeEnum;
import com.ctrip.framework.drc.console.exception.ConsoleException;
import com.ctrip.framework.drc.console.param.v2.MhaReplicationQuery;
import com.ctrip.framework.drc.console.service.v2.MhaReplicationServiceV2;
import com.ctrip.framework.drc.console.service.v2.MysqlServiceV2;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.console.utils.StreamUtils;
import com.ctrip.framework.drc.core.http.PageResult;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by dengquanliang
 * 2023/5/25 14:09
 */
@Service
public class MhaReplicationServiceV2Impl implements MhaReplicationServiceV2 {

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


    @Override
    public List<MhaReplicationDto> queryRelatedReplications(String mhaName, List<String> dbNames) {
        try {
            //oldMha + dbs -> related dbTbls, dbMhaMapping
            MhaTblV2 mhaTblV2 = mhaTblV2Dao.queryByMhaName(mhaName);
            List<DbTbl> dbTbls = dbTblDao.queryByDbNames(dbNames);
            List<Long> dbIds = dbTbls.stream().map(DbTbl::getId).collect(Collectors.toList());
            List<MhaDbMappingTbl> mhaDbMappingTbls = mhaDbMappingTblDao.queryByDbIdsAndMhaId(dbIds, mhaTblV2.getId());

            //dbReplications, mhaReplications
            List<Long> mappingIds = mhaDbMappingTbls.stream().map(MhaDbMappingTbl::getId).collect(Collectors.toList());
            List<DbReplicationTbl> srcReplication = dbReplicationTblDao.queryBySrcMappingIds(mappingIds, ReplicationTypeEnum.DB_TO_DB.getType())
                    .stream().filter(StreamUtils.distinctByKey(p -> p.getSrcMhaDbMappingId() + "," + p.getDstMhaDbMappingId())).collect(Collectors.toList());
            List<DbReplicationTbl> dstReplication = dbReplicationTblDao.queryByDestMappingIds(mappingIds, ReplicationTypeEnum.DB_TO_DB.getType())
                    .stream().filter(StreamUtils.distinctByKey(p -> p.getSrcMhaDbMappingId() + "," + p.getDstMhaDbMappingId())).collect(Collectors.toList());


            List<MhaReplicationTbl> tblRes = Lists.newArrayList();
            tblRes.addAll(this.queryMhaReplicationFromDbReplication(srcReplication));
            tblRes.addAll(this.queryMhaReplicationFromDbReplication(dstReplication));

            List<Long> mhaIds = Lists.newArrayList();
            mhaIds.addAll(tblRes.stream().map(MhaReplicationTbl::getDstMhaId).collect(Collectors.toList()));
            mhaIds.addAll(tblRes.stream().map(MhaReplicationTbl::getSrcMhaId).collect(Collectors.toList()));
            List<MhaTblV2> mhaTblV2List = mhaTblV2Dao.queryByIds(mhaIds);
            Map<Long, MhaTblV2> mhaMap = mhaTblV2List.stream().collect(Collectors.toMap(MhaTblV2::getId, Function.identity()));

            return tblRes.stream().map(e -> MhaReplicationDto.from(e, mhaMap)).collect(Collectors.toList());
        } catch (SQLException e) {
            logger.error("queryRelatedReplications error", e);
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
    }

    private List<MhaReplicationTbl> queryMhaReplicationFromDbReplication(List<DbReplicationTbl> srcReplication) throws SQLException {
        List<Long> allMappingIds = Lists.newArrayList();
        allMappingIds.addAll(srcReplication.stream().map(DbReplicationTbl::getSrcMhaDbMappingId).collect(Collectors.toList()));
        allMappingIds.addAll(srcReplication.stream().map(DbReplicationTbl::getDstMhaDbMappingId).collect(Collectors.toList()));

        List<MhaDbMappingTbl> mhaDbMappingTbls = mhaDbMappingTblDao.queryByIds(allMappingIds);
        Map<Long, MhaDbMappingTbl> mappingTblMap = mhaDbMappingTbls.stream().collect(Collectors.toMap(MhaDbMappingTbl::getId, Function.identity()));
        List<MhaReplicationTbl> res = Lists.newArrayList();
        Set<String> set = Sets.newHashSet();
        for (DbReplicationTbl dbReplicationTbl : srcReplication) {
            MhaDbMappingTbl srcMapping = mappingTblMap.get(dbReplicationTbl.getSrcMhaDbMappingId());
            MhaDbMappingTbl dstMapping = mappingTblMap.get(dbReplicationTbl.getDstMhaDbMappingId());
            // todo by yongnian: 2023/8/23 不要数据库循环查
            String key = srcMapping.getMhaId() + "-" + dstMapping.getMhaId();
            if (set.contains(key)) {
                continue;
            }
            set.add(key);
            res.add(mhaReplicationTblDao.queryByMhaId(srcMapping.getMhaId(), dstMapping.getMhaId()));
        }
        return res;
    }


    @Override
    public MhaDelayInfoDto getMhaReplicationDelay(String srcMha, String dstMha) {
        try {
            // check
            List<MhaTblV2> mhaTblV2List = mhaTblV2Dao.queryByMhaNames(Lists.newArrayList(srcMha, dstMha));
            MhaTblV2 srcMhaTbl = mhaTblV2List.stream().filter(e -> e.getMhaName().equals(srcMha)).findAny()
                    .orElseThrow(() -> ConsoleExceptionUtils.message(ReadableErrorDefEnum.REQUEST_PARAM_INVALID, "mha not found: " + srcMha));
            MhaTblV2 dstMhaTbl = mhaTblV2List.stream().filter(e -> e.getMhaName().equals(dstMha)).findAny()
                    .orElseThrow(() -> ConsoleExceptionUtils.message(ReadableErrorDefEnum.REQUEST_PARAM_INVALID, "mha not found: " + dstMha));

            // query dst first, result could be larger than original
            long start = System.currentTimeMillis();
            Long dstTime = mysqlServiceV2.getDelayUpdateTime(dstMha, srcMha);
            logger.info("[delay query] dstMha:{}, dstTime:{}, cost:{}", dstMha, dstTime, System.currentTimeMillis() - start);

            start = System.currentTimeMillis();
            Long srcTime = mysqlServiceV2.getCurrentTime(srcMha);
            logger.info("[delay query] srcMha:{}, srcTime:{}, cost:{}", srcMha, srcTime, System.currentTimeMillis() - start);

            Long delay = null;
            if (srcTime != null && dstTime != null) {
                delay = srcTime - dstTime;
            }
            MhaDelayInfoDto delayInfoDto = new MhaDelayInfoDto();
            delayInfoDto.setSrcMha(srcMha);
            delayInfoDto.setDstMha(dstMha);
            delayInfoDto.setDelay(delay);
            return delayInfoDto;
        } catch (Exception e) {
            if (e instanceof ConsoleException) {
                throw (ConsoleException) e;
            }
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
    }


    @Override
    public List<MhaDelayInfoDto> getMhaReplicationDelays(List<MhaReplicationDto> mhaReplicationDtoList) {
        List<Callable<MhaDelayInfoDto>> list = Lists.newArrayList();
        for (MhaReplicationDto dto : mhaReplicationDtoList) {
            list.add(() -> this.getMhaReplicationDelay(dto.getSrcMha().getName(), dto.getDstMha().getName()));
        }

        try {
            List<MhaDelayInfoDto> res = Lists.newArrayList();
            List<Future<MhaDelayInfoDto>> futures = executorService.invokeAll(list, 3, TimeUnit.SECONDS);
            for (Future<MhaDelayInfoDto> future : futures) {
                res.add(future.get());
            }
            return res;
        } catch (InterruptedException | ExecutionException e) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.TIMEOUT_EXCEPTION, e.getMessage());
        }
    }
}
