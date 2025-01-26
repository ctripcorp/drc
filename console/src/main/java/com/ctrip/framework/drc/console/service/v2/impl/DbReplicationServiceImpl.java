package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.*;
import com.ctrip.framework.drc.console.dao.entity.DbTbl;
import com.ctrip.framework.drc.console.dao.entity.DcTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.DbReplicationTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaDbMappingTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.*;
import com.ctrip.framework.drc.console.enums.ReadableErrorDefEnum;
import com.ctrip.framework.drc.core.meta.ReplicationTypeEnum;
import com.ctrip.framework.drc.console.param.v2.MqReplicationQuery;
import com.ctrip.framework.drc.console.service.v2.*;
import com.ctrip.framework.drc.console.service.v2.external.dba.DbaApiService;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.console.vo.display.v2.DbReplicationVo;
import com.ctrip.framework.drc.console.vo.request.MqReplicationQueryDto;
import com.ctrip.framework.drc.core.http.PageResult;
import com.ctrip.framework.drc.core.monitor.util.ServicesUtil;
import com.ctrip.framework.drc.core.service.user.IAMService;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by shiruixin
 * 2024/8/27 11:37
 */
@Service
public class DbReplicationServiceImpl implements DbReplicationService {
    private static final Logger logger = LoggerFactory.getLogger(DbReplicationServiceImpl.class);
    private IAMService iamService = ServicesUtil.getIAMService();

    @Autowired
    private MhaDbMappingTblDao mhaDbMappingTblDao;
    @Autowired
    private DbReplicationTblDao dbReplicationTblDao;
    @Autowired
    private DbTblDao dbTblDao;
    @Autowired
    private MhaTblV2Dao mhaTblV2Dao;
    @Autowired
    private DefaultConsoleConfig defaultConsoleConfig;
    @Autowired
    private DcTblDao dcTblDao;
    @Autowired
    private DbaApiService dbaApiService;

    @Override
    public PageResult<DbReplicationVo> queryMqReplicationsByPage(MqReplicationQueryDto queryDto) throws SQLException {
        MqReplicationQuery query = new MqReplicationQuery();
        query.setPageIndex(queryDto.getPageIndex());
        query.setPageSize(queryDto.getPageSize());

        String mqPanelUrlPrefix = defaultConsoleConfig.getConsoleMqPanelUrl();

        Pair<Boolean, List<String>> adminAndDbs = getPermissionAndDbsCanQuery();

        if (!adminAndDbs.getLeft()) {
            if (CollectionUtils.isEmpty(adminAndDbs.getRight())) {
                return PageResult.emptyResult();
            } else {
                List<DbTbl> relatedDbs = dbTblDao.queryByDbNames(adminAndDbs.getRight());
                List<Long> dbIds = relatedDbs.stream().map(DbTbl::getId).collect(Collectors.toList());
                List<MhaDbMappingTbl> relatedMhaDbMappings = mhaDbMappingTblDao.queryByDbIds(dbIds);
                if (CollectionUtils.isEmpty(relatedMhaDbMappings)) {
                    return PageResult.emptyResult();
                }
                List<Long> mhaDbMappingTblIds = relatedMhaDbMappings.stream().map(MhaDbMappingTbl::getId).collect(Collectors.toList());
                query.addOrIntersectSrcMhaDbMappingIds(mhaDbMappingTblIds);
            }
        }

        query.setReplicationType(queryDto.getMqTypeEnum().getReplicationType().getType());

        try {
            // search only otter messenger
            query.setQueryOtter(queryDto.isQueryOtter());
            // query by topics
            if (!StringUtils.isEmpty(queryDto.getTopic())) {
                List<String> topics = Arrays.stream(queryDto.getTopic().split(","))
                        .map(String::trim)
                        .filter( e -> !StringUtils.isEmpty(e))
                        .collect(Collectors.toList());
                query.setTopicLikePatterns(topics.stream().collect(Collectors.toSet()));
            }

            // query by srcTbls
            if (!StringUtils.isEmpty(queryDto.getSrcTblName())) {
                List<String> tblNames = Arrays.stream(queryDto.getSrcTblName().split(","))
                        .map(String::trim)
                        .filter( e -> !StringUtils.isEmpty(e))
                        .collect(Collectors.toList());
                query.setSrcTableLikePatterns(tblNames.stream().collect(Collectors.toSet()));
            }
            // query by dbnames
            if (!StringUtils.isBlank(queryDto.getDbNames())) {
                List<String> dbNames = Arrays.stream(queryDto.getDbNames().split(","))
                        .map(String::trim)
                        .filter( e -> !StringUtils.isEmpty(e))
                        .collect(Collectors.toList());
                List<DbTbl> relatedDbs = dbTblDao.queryByDbNames(dbNames);
                if (CollectionUtils.isEmpty(relatedDbs)) {
                    return PageResult.emptyResult();
                }
                List<Long> dbIds = relatedDbs.stream().map(DbTbl::getId).collect(Collectors.toList());

                List<MhaDbMappingTbl> relatedMhaDbMappings = mhaDbMappingTblDao.queryByDbIds(dbIds);
                List<Long> mhaDbMappingTblIds = relatedMhaDbMappings.stream().map(MhaDbMappingTbl::getId).collect(Collectors.toList());

                query.addOrIntersectSrcMhaDbMappingIds(mhaDbMappingTblIds);
                if (CollectionUtils.isEmpty(query.getSrcMhaDbMappingIdList())) {
                    return PageResult.emptyResult();
                }
            }

            // query replication
            PageResult<DbReplicationTbl> tblPageResult = this.queryByPage(query);
            List<DbReplicationTbl> data = tblPageResult.getData();
            if (tblPageResult.getTotalCount() == 0) {
                return PageResult.emptyResult();
            }

            // query mha name, db name, dc name information
            List<Long> srcMhaDbMappingIds = data.stream().map(DbReplicationTbl::getSrcMhaDbMappingId)
                    .collect(Collectors.toList());
            List<MhaDbMappingTbl> mhaDbMappingTbls = mhaDbMappingTblDao.queryByIds(srcMhaDbMappingIds);
            Map<Long,MhaDbMappingTbl> mhaDbMappingMap = mhaDbMappingTbls.stream().collect(Collectors.toMap(MhaDbMappingTbl::getId, e -> e));
            List<Long> mhaIds = mhaDbMappingTbls.stream().map(MhaDbMappingTbl::getMhaId)
                    .collect(Collectors.toList());
            List<Long> dbIds = mhaDbMappingTbls.stream().map(MhaDbMappingTbl::getDbId)
                    .collect(Collectors.toList());
            List<MhaTblV2> mhaTblV2s = mhaTblV2Dao.queryByIds(mhaIds);
            Map<Long,MhaTblV2> mhaTblMap = mhaTblV2s.stream().collect(Collectors.toMap(MhaTblV2::getId, e -> e));
            List<DbTbl> dbTbls = dbTblDao.queryByIds(dbIds);
            Map<Long,DbTbl> dbMap = dbTbls.stream().collect(Collectors.toMap(DbTbl::getId, e -> e));
            List<DcTbl> dcTbls = dcTblDao.queryAll();
            Map<Long,DcTbl> dcMap = dcTbls.stream().collect(Collectors.toMap(DcTbl::getId, e -> e));

            List<DbReplicationVo> result = data.stream().map( e -> {
                DbReplicationVo dbReplicationVo = new DbReplicationVo();
                dbReplicationVo.setDbReplicationId(e.getId());
                dbReplicationVo.setSrcLogicTableName(e.getSrcLogicTableName());
                dbReplicationVo.setDstLogicTableName(e.getDstLogicTableName());

                MhaDbMappingTbl mhaDbMappingTbl = mhaDbMappingMap.get(e.getSrcMhaDbMappingId());
                DbTbl dbTbl = dbMap.get(mhaDbMappingTbl.getDbId());
                MhaTblV2 mhaTblV2 = mhaTblMap.get(mhaDbMappingTbl.getMhaId());
                DcTbl dcTbl = dcMap.get(mhaTblV2.getDcId());
                dbReplicationVo.setDbName(dbTbl.getDbName());
                dbReplicationVo.setMhaName(mhaTblV2.getMhaName());
                dbReplicationVo.setDcName(dcTbl.getDcName());

                dbReplicationVo.setMqPanelUrl(mqPanelUrlPrefix + "&var-mha=" + mhaTblV2.getMhaName() + "&var-auto_gen_other_mha=All");

                return dbReplicationVo;
            }).collect(Collectors.toList());

            return PageResult.newInstance(result, tblPageResult.getPageIndex(), tblPageResult.getPageSize(), tblPageResult.getTotalCount());

        } catch (Exception e) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_MQ_REPLICATION_FAIL, e);
        }
    }

    @Override
    public PageResult<DbReplicationTbl> queryByPage(MqReplicationQuery query) {
        try {
            List<DbReplicationTbl> data = dbReplicationTblDao.queryByPage(query);
            int count = dbReplicationTblDao.count(query);
            return PageResult.newInstance(data, query.getPageIndex(), query.getPageSize(), count);
        } catch (SQLException e){
            logger.error("queryByPage error", e);
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
    }

    private Pair<Boolean, List<String>> getPermissionAndDbsCanQuery() {
        if (!iamService.canQueryAllDbReplication().getLeft()) {
            List<String> dbsCanQuery = dbaApiService.getDBsWithQueryPermission();
            if (CollectionUtils.isEmpty(dbsCanQuery)) {
                throw ConsoleExceptionUtils.message("no db with DOT permission!");
            }
            return Pair.of(false, dbsCanQuery);
        } else {
            return Pair.of(true, Lists.newArrayList());
        }
    }
}
