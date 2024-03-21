package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.DbTblDao;
import com.ctrip.framework.drc.console.dao.DcTblDao;
import com.ctrip.framework.drc.console.dao.entity.DbTbl;
import com.ctrip.framework.drc.console.dao.entity.DcTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.DbReplicationFilterMappingTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.DbReplicationTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaDbMappingTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.entity.v3.*;
import com.ctrip.framework.drc.console.dao.v2.DbReplicationFilterMappingTblDao;
import com.ctrip.framework.drc.console.dao.v2.DbReplicationTblDao;
import com.ctrip.framework.drc.console.dao.v2.MhaDbMappingTblDao;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.dao.v3.*;
import com.ctrip.framework.drc.console.dto.v2.MhaDbDelayInfoDto;
import com.ctrip.framework.drc.console.dto.v3.DbReplicationDto;
import com.ctrip.framework.drc.console.dto.v3.LogicTableConfig;
import com.ctrip.framework.drc.console.dto.v3.MhaDbDto;
import com.ctrip.framework.drc.console.dto.v3.MhaDbReplicationDto;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.ReadableErrorDefEnum;
import com.ctrip.framework.drc.console.enums.ReplicationTypeEnum;
import com.ctrip.framework.drc.console.enums.TransmissionTypeEnum;
import com.ctrip.framework.drc.console.param.mysql.DrcDbMonitorTableCreateReq;
import com.ctrip.framework.drc.console.param.v2.MhaDbReplicationQuery;
import com.ctrip.framework.drc.console.pojo.domain.DcDo;
import com.ctrip.framework.drc.console.service.v2.MetaInfoServiceV2;
import com.ctrip.framework.drc.console.service.v2.MhaDbMappingService;
import com.ctrip.framework.drc.console.service.v2.MhaDbReplicationService;
import com.ctrip.framework.drc.console.service.v2.MysqlServiceV2;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.console.utils.MultiKey;
import com.ctrip.framework.drc.console.utils.NumberUtils;
import com.ctrip.framework.drc.console.utils.StreamUtils;
import com.ctrip.framework.drc.console.vo.request.MhaDbQueryDto;
import com.ctrip.framework.drc.console.vo.request.MhaDbReplicationQueryDto;
import com.ctrip.framework.drc.core.http.PageResult;
import com.ctrip.framework.drc.core.server.common.filter.table.aviator.AviatorRegexFilter;
import com.ctrip.platform.dal.dao.annotation.DalTransactional;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.ctrip.framework.drc.console.utils.StreamUtils.getKey;

@Service
public class MhaDbReplicationServiceImpl implements MhaDbReplicationService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private MhaDbMappingTblDao mhaDbMappingTblDao;
    @Autowired
    private MhaTblV2Dao mhaTblV2Dao;
    @Autowired
    private DbTblDao dbTblDao;
    @Autowired
    private DcTblDao dcTblDao;
    @Autowired
    private MhaDbReplicationTblDao mhaDbReplicationTblDao;
    @Autowired
    private DbReplicationTblDao dbReplicationTblDao;
    @Autowired
    private MysqlServiceV2 mysqlServiceV2;
    @Autowired
    private DefaultConsoleConfig defaultConsoleConfig;
    @Autowired
    private ApplierTblV3Dao applierTblV3Dao;
    @Autowired
    private ApplierGroupTblV3Dao applierGroupTblV3Dao;
    @Autowired
    private MessengerTblV3Dao messengerTblV3Dao;
    @Autowired
    private MessengerGroupTblV3Dao messengerGroupTblV3Dao;
    @Autowired
    private MhaDbMappingService mhaDbMappingService;
    @Autowired
    private MetaInfoServiceV2 metaInfoServiceV2;
    @Autowired
    private DbReplicationFilterMappingTblDao dbReplicationFilterMappingTblDao;


    @Override
    public List<MhaDbReplicationDto> queryByMha(String srcMhaName, String dstMhaName, List<String> dbNames) {
        try {
            List<MhaDbReplicationTbl> replicationTbls = this.getMhaDbReplicationTbls(srcMhaName, dstMhaName, dbNames);
            return this.convert(replicationTbls);
        } catch (SQLException e) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
    }

    private List<MhaDbReplicationTbl> getMhaDbReplicationTbls(String srcMhaName, String dstMhaName, List<String> dbNames) throws SQLException {
        // 1. query mhaDbMapping by conditions
        MhaTblV2 srcMhaTbl = mhaTblV2Dao.queryByMhaName(srcMhaName, 0);
        MhaTblV2 dstMhaTbl = mhaTblV2Dao.queryByMhaName(dstMhaName, 0);
        if (srcMhaTbl == null || dstMhaTbl == null) {
            return Collections.emptyList();
        }

        List<MhaDbMappingTbl> mhaDbMappingTbls;
        List<DbTbl> dbTbls;
        if (CollectionUtils.isEmpty(dbNames)) {
            mhaDbMappingTbls = mhaDbMappingTblDao.queryByMhaIds(Lists.newArrayList(srcMhaTbl.getId(), dstMhaTbl.getId()));
        } else {
            dbTbls = dbTblDao.queryByDbNames(dbNames);
            List<Long> dbIds = dbTbls.stream().map(DbTbl::getId).distinct().collect(Collectors.toList());
            mhaDbMappingTbls = mhaDbMappingTblDao.queryByDbIdsAndMhaIds(dbIds, Lists.newArrayList(srcMhaTbl.getId(), dstMhaTbl.getId()));
        }
        List<Long> srcMappingIds = mhaDbMappingTbls.stream().filter(e -> e.getMhaId().equals(srcMhaTbl.getId())).map(MhaDbMappingTbl::getId).collect(Collectors.toList());
        List<Long> dstMappingIds = mhaDbMappingTbls.stream().filter(e -> e.getMhaId().equals(dstMhaTbl.getId())).map(MhaDbMappingTbl::getId).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(srcMappingIds) || CollectionUtils.isEmpty(dstMappingIds)) {
            return Collections.emptyList();
        }

        // 2 query dbReplications/mhaDbReplication by (src -> dst) mapping id
        MhaDbReplicationQuery mhaDbReplicationQuery = new MhaDbReplicationQuery();
        mhaDbReplicationQuery.setSrcMappingIdList(srcMappingIds);
        mhaDbReplicationQuery.setDstMappingIdList(dstMappingIds);
        mhaDbReplicationQuery.setType(ReplicationTypeEnum.DB_TO_DB.getType());
        return mhaDbReplicationTblDao.query(mhaDbReplicationQuery);
    }

    private List<DbReplicationTbl> getDbReplicationTbls(List<MhaDbReplicationTbl> replicationTbls) throws SQLException {
        List<DbReplicationTbl> samples = replicationTbls.stream().map(e -> {
            DbReplicationTbl tbl = new DbReplicationTbl();
            tbl.setSrcMhaDbMappingId(e.getSrcMhaDbMappingId());
            tbl.setDstMhaDbMappingId(e.getDstMhaDbMappingId());
            tbl.setReplicationType(e.getReplicationType());
            return tbl;
        }).collect(Collectors.toList());
        return dbReplicationTblDao.queryBySamples(samples);
    }

    @Override
    public List<MhaDbReplicationDto> queryMqByMha(String srcMhaName, List<String> dbNames) {
        try {
            // 1. query mhaDbMapping by conditions
            MhaTblV2 srcMhaTbl = mhaTblV2Dao.queryByMhaName(srcMhaName, 0);
            if (srcMhaTbl == null) {
                return Collections.emptyList();
            }

            List<MhaDbMappingTbl> mhaDbMappingTbls;
            List<DbTbl> dbTbls;
            if (CollectionUtils.isEmpty(dbNames)) {
                mhaDbMappingTbls = mhaDbMappingTblDao.queryByMhaId(srcMhaTbl.getId());
                List<Long> dbIds = mhaDbMappingTbls.stream().map(MhaDbMappingTbl::getDbId).distinct().collect(Collectors.toList());
                dbTbls = dbTblDao.queryByIds(dbIds);
            } else {
                dbTbls = dbTblDao.queryByDbNames(dbNames);
                List<Long> dbIds = dbTbls.stream().map(DbTbl::getId).distinct().collect(Collectors.toList());
                mhaDbMappingTbls = mhaDbMappingTblDao.queryByDbIdsAndMhaIds(dbIds, Lists.newArrayList(srcMhaTbl.getId()));
            }
            if (CollectionUtils.isEmpty(mhaDbMappingTbls)) {
                return Collections.emptyList();
            }
            List<Long> ids = mhaDbMappingTbls.stream().map(MhaDbMappingTbl::getId).collect(Collectors.toList());

            // 2 query dbReplications/mhaDbReplication by (src -> dst) mapping id
            MhaDbReplicationQuery mhaDbReplicationQuery = new MhaDbReplicationQuery();
            mhaDbReplicationQuery.setSrcMappingIdList(ids);
            mhaDbReplicationQuery.setDstMappingIdList(Lists.newArrayList(-1L));
            mhaDbReplicationQuery.setType(ReplicationTypeEnum.DB_TO_MQ.getType());
            List<MhaDbReplicationTbl> replicationTbls = mhaDbReplicationTblDao.query(mhaDbReplicationQuery);

            return this.convert(replicationTbls);
        } catch (SQLException e) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
    }

    @Override
    public List<MhaDbReplicationDto> queryByDcName(String srcDcName, ReplicationTypeEnum typeEnum) {
        try {
            // 1. mhas
            DcTbl dcTbl = dcTblDao.queryByDcName(srcDcName);
            if (dcTbl == null) {
                return Collections.emptyList();
            }
            List<MhaTblV2> srcMhaTbls = mhaTblV2Dao.queryByDcId(dcTbl.getId(), BooleanEnum.FALSE.getCode());

            List<Long> srcMhaIdList = srcMhaTbls.stream().map(MhaTblV2::getId).collect(Collectors.toList());
            List<MhaDbMappingTbl> srcMhaDbMappingTbls = mhaDbMappingTblDao.queryByMhaIds(srcMhaIdList);
            if (CollectionUtils.isEmpty(srcMhaDbMappingTbls)) {
                return Collections.emptyList();
            }
            List<Long> srcMappingIdList = srcMhaDbMappingTbls.stream().map(MhaDbMappingTbl::getId).collect(Collectors.toList());

            // 2 query dbReplications/mhaDbReplication by (src -> dst) mapping id
            MhaDbReplicationQuery mhaDbReplicationQuery = new MhaDbReplicationQuery();
            mhaDbReplicationQuery.setSrcMappingIdList(srcMappingIdList);
            if (typeEnum != null) {
                mhaDbReplicationQuery.setType(typeEnum.getType());
            }
            List<MhaDbReplicationTbl> replicationTbls = mhaDbReplicationTblDao.query(mhaDbReplicationQuery);
            if (CollectionUtils.isEmpty(replicationTbls)) {
                return Collections.emptyList();
            }
            // 3. build
            // 3.0 query rest data for build
            Set<Long> srcMappingIdSet = Sets.newHashSet(srcMappingIdList);
            List<Long> restMappingIds = replicationTbls.stream().map(MhaDbReplicationTbl::getDstMhaDbMappingId).filter(e -> !srcMappingIdSet.contains(e)).collect(Collectors.toList());
            List<MhaDbMappingTbl> restMhaDbMappingTbls = mhaDbMappingTblDao.queryByIds(restMappingIds);

            Set<Long> srcMhaIdSet = Sets.newHashSet(srcMhaIdList);
            List<Long> restMhaIds = restMhaDbMappingTbls.stream().map(MhaDbMappingTbl::getMhaId).filter(e -> !srcMhaIdSet.contains(e)).collect(Collectors.toList());
            List<MhaTblV2> dstMhaTbls = mhaTblV2Dao.queryByIds(restMhaIds);
            // 3.1 mha data
            List<MhaTblV2> mhaTbls = Lists.newArrayList();
            mhaTbls.addAll(srcMhaTbls);
            mhaTbls.addAll(dstMhaTbls);
            Map<Long, String> mhaIdNameMap = mhaTbls.stream().filter(StreamUtils.distinctByKey(MhaTblV2::getId)).collect(Collectors.toMap(MhaTblV2::getId, MhaTblV2::getMhaName));

            // 3.2 mapping data
            List<MhaDbMappingTbl> mappingTbls = Lists.newArrayList();
            mappingTbls.addAll(srcMhaDbMappingTbls);
            mappingTbls.addAll(restMhaDbMappingTbls);

            // 3.3 db data
            List<Long> dbIds = mappingTbls.stream().map(MhaDbMappingTbl::getDbId).distinct().collect(Collectors.toList());
            List<DbTbl> dbTbls = dbTblDao.queryByIds(dbIds);
            Map<Long, String> dbMap = dbTbls.stream().collect(Collectors.toMap(DbTbl::getId, DbTbl::getDbName));

            // 3.4 mapping id -> mha/db name
            Map<Long, String> mhaNameMap = mappingTbls.stream().collect(Collectors.toMap(MhaDbMappingTbl::getId, e -> mhaIdNameMap.get(e.getMhaId())));
            Map<Long, String> dbNameMap = mappingTbls.stream().collect(Collectors.toMap(MhaDbMappingTbl::getId, e -> dbMap.get(e.getDbId())));

            List<MhaDbReplicationDto> res = replicationTbls.stream().map(e -> {
                MhaDbReplicationDto dto = new MhaDbReplicationDto();
                dto.setId(e.getId());
                dto.setSrc(new MhaDbDto(e.getSrcMhaDbMappingId(), Objects.requireNonNull(mhaNameMap.get(e.getSrcMhaDbMappingId())), Objects.requireNonNull(dbNameMap.get(e.getSrcMhaDbMappingId()))));
                if (ReplicationTypeEnum.DB_TO_MQ.getType().equals(e.getReplicationType())) {
                    dto.setDst(MhaDbReplicationDto.MQ_DTO);
                } else {
                    dto.setDst(new MhaDbDto(e.getDstMhaDbMappingId(), Objects.requireNonNull(mhaNameMap.get(e.getDstMhaDbMappingId())), Objects.requireNonNull(dbNameMap.get(e.getDstMhaDbMappingId()))));
                }
                dto.setReplicationType(e.getReplicationType());
                return dto;
            }).collect(Collectors.toList());
            this.fillDrcStatus(res);
            return res;
        } catch (SQLException e) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
    }

    @Override
    public PageResult<MhaDbReplicationDto> query(MhaDbReplicationQueryDto queryDto) {
        try {
            // convert query conditions
            MhaDbReplicationQuery query = new MhaDbReplicationQuery();
            MhaDbQueryDto src = queryDto.getSrcMhaDb();
            if (src != null && src.isConditionalQuery()) {
                List<MhaDbMappingTbl> list = mhaDbMappingService.query(src);
                if (CollectionUtils.isEmpty(list)) {
                    return PageResult.emptyResult();
                }
                query.setSrcMappingIdList(list.stream().map(MhaDbMappingTbl::getId).collect(Collectors.toList()));
            }
            MhaDbQueryDto dst = queryDto.getDstMhaDb();
            if (dst != null && dst.isConditionalQuery()) {
                List<MhaDbMappingTbl> list = mhaDbMappingService.query(dst);
                if (CollectionUtils.isEmpty(list)) {
                    return PageResult.emptyResult();
                }
                query.setDstMappingIdList(list.stream().map(MhaDbMappingTbl::getId).collect(Collectors.toList()));
            }
            MhaDbQueryDto related = queryDto.getRelatedMhaDb();
            if (related != null && related.isConditionalQuery()) {
                List<MhaDbMappingTbl> list = mhaDbMappingService.query(related);
                if (CollectionUtils.isEmpty(list)) {
                    return PageResult.emptyResult();
                }
                query.setRelatedMappingList(list.stream().map(MhaDbMappingTbl::getId).collect(Collectors.toList()));
            }
            // drc status
            if (queryDto.getDrcStatus() != null) {
                List<ApplierTblV3> applierTblV3s = applierTblV3Dao.queryAllExist();
                List<Long> applierGroupIds = applierTblV3s.stream().map(ApplierTblV3::getApplierGroupId).distinct().collect(Collectors.toList());
                List<ApplierGroupTblV3> applierGroupTblV3s = applierGroupTblV3Dao.queryByIds(applierGroupIds);
                List<Long> replicationIds = applierGroupTblV3s.stream().map(ApplierGroupTblV3::getMhaDbReplicationId).collect(Collectors.toList());
                if (Objects.equals(BooleanEnum.TRUE.getCode(), queryDto.getDrcStatus())) {
                    if (CollectionUtils.isEmpty(replicationIds)) {
                        return PageResult.emptyResult();
                    }
                    query.setIdList(replicationIds);
                } else {
                    query.setExcludeIdList(replicationIds);
                }
            }

            query.setType(ReplicationTypeEnum.DB_TO_DB.getType());
            query.setPageIndex(queryDto.getPageIndex());
            query.setPageSize((queryDto.getPageSize()));
            List<MhaDbReplicationTbl> replicationTbls = mhaDbReplicationTblDao.queryByPage(query);
            int count = mhaDbReplicationTblDao.count(query);
            // tbl -> dto
            List<MhaDbReplicationDto> replicationDtos = this.convert(replicationTbls);
            return PageResult.newInstance(replicationDtos, queryDto.getPageIndex(), queryDto.getPageSize(), count);
        } catch (SQLException e) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
    }

    private List<MhaDbReplicationDto> convert(List<MhaDbReplicationTbl> replicationTbls) throws SQLException {
        List<Long> ids = replicationTbls.stream().flatMap(e -> Stream.of(e.getSrcMhaDbMappingId(), e.getDstMhaDbMappingId())).collect(Collectors.toList());
        List<MhaDbMappingTbl> mappingTbls = mhaDbMappingTblDao.queryByIds(ids);
        List<MhaTblV2> mhaTbls = mhaTblV2Dao.queryByIds(mappingTbls.stream().map(MhaDbMappingTbl::getMhaId).collect(Collectors.toList()));
        List<DbTbl> dbTbls = dbTblDao.queryByIds(mappingTbls.stream().map(MhaDbMappingTbl::getDbId).collect(Collectors.toList()));

        Map<Long, MhaTblV2> mhaIdNameMap = mhaTbls.stream().collect(Collectors.toMap(MhaTblV2::getId, e -> e));
        Map<Long, DbTbl> dbMap = dbTbls.stream().collect(Collectors.toMap(DbTbl::getId, e -> e));

        Map<Long, MhaTblV2> mhaNameMap = mappingTbls.stream().collect(Collectors.toMap(MhaDbMappingTbl::getId, e -> mhaIdNameMap.get(e.getMhaId())));
        Map<Long, DbTbl> dbNameMap = mappingTbls.stream().collect(Collectors.toMap(MhaDbMappingTbl::getId, e -> dbMap.get(e.getDbId())));

        List<DcDo> dcDos = metaInfoServiceV2.queryAllDcWithCache();
        Map<Long, DcDo> dcDoMap = dcDos.stream().collect(Collectors.toMap(DcDo::getDcId, e -> e));

        List<DbReplicationTbl> dbReplicationTbls = this.getDbReplicationTbls(replicationTbls);
        Map<MultiKey, List<DbReplicationTbl>> dbReplicationsByKey = dbReplicationTbls.stream().collect(Collectors.groupingBy(StreamUtils::getKey));

        List<MhaDbReplicationDto> res = replicationTbls.stream().map(e -> {
            MhaDbReplicationDto dto = new MhaDbReplicationDto();
            dto.setId(e.getId());
            dto.setSrc(getMhaDbDto(e.getSrcMhaDbMappingId(), mhaNameMap, dbNameMap, dcDoMap));
            if (ReplicationTypeEnum.DB_TO_MQ.getType().equals(e.getReplicationType())) {
                dto.setDst(MhaDbReplicationDto.MQ_DTO);
            } else {
                dto.setDst(getMhaDbDto(e.getDstMhaDbMappingId(), mhaNameMap, dbNameMap, dcDoMap));
            }
            dto.setReplicationType(e.getReplicationType());
            List<DbReplicationTbl> dbReplicationTblList = dbReplicationsByKey.getOrDefault(getKey(e), Collections.emptyList());
            dto.setDbReplicationDtos(convertTo(dbReplicationTblList));
            return dto;
        }).collect(Collectors.toList());
        this.fillDrcStatus(res);
        this.fillTransmissionType(res);
        return res;
    }

    public List<DbReplicationDto> convertTo(List<DbReplicationTbl> existDbReplications) {
        try {
            if (CollectionUtils.isEmpty(existDbReplications)) {
                return Collections.emptyList();
            }
            List<Long> dbReplicationIds = existDbReplications.stream().map(DbReplicationTbl::getId).collect(Collectors.toList());
            List<DbReplicationFilterMappingTbl> filterMappingTbls = dbReplicationFilterMappingTblDao.queryByDbReplicationIds(dbReplicationIds);
            Map<Long, DbReplicationFilterMappingTbl> filterMappingTblsMap = filterMappingTbls.stream().collect(Collectors.toMap(DbReplicationFilterMappingTbl::getDbReplicationId, Function.identity()));

            return existDbReplications.stream().map(source -> {
                DbReplicationFilterMappingTbl filterMapping = filterMappingTblsMap.get(source.getId());
                LogicTableConfig logicTableConfig = new LogicTableConfig();
                logicTableConfig.setLogicTable(source.getSrcLogicTableName());
                if (filterMapping != null) {
                    if (filterMapping.getRowsFilterId() != -1L) {
                        logicTableConfig.setRowsFilterId(filterMapping.getRowsFilterId());
                    }
                    if (filterMapping.getColumnsFilterId() != -1) {
                        logicTableConfig.setColsFilterId(filterMapping.getColumnsFilterId());
                    }
                }
                return new DbReplicationDto(source.getId(), logicTableConfig);

            }).collect(Collectors.toList());
        } catch (SQLException e) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
    }

    private void fillTransmissionType(List<MhaDbReplicationDto> res) throws SQLException {
        // simplex or duplex
        Map<MultiKey, MhaDbReplicationTbl> reverseMap = res.stream().collect(Collectors.toMap(StreamUtils::getKey, e -> {
            MhaDbReplicationTbl tbl = new MhaDbReplicationTbl();
            tbl.setSrcMhaDbMappingId(e.getDst().getMhaDbMappingId());
            tbl.setDstMhaDbMappingId(e.getSrc().getMhaDbMappingId());
            tbl.setReplicationType(e.getReplicationType());
            return tbl;
        }));
        List<MhaDbReplicationTbl> mhaDbReplicationTbls = mhaDbReplicationTblDao.queryBySamples(Lists.newArrayList(reverseMap.values()));
        Set<MultiKey> keys = mhaDbReplicationTbls.stream().map(StreamUtils::getReverseKey).collect(Collectors.toSet());
        res.forEach(e -> {
            TransmissionTypeEnum type = keys.contains(getKey(e)) ? TransmissionTypeEnum.DUPLEX : TransmissionTypeEnum.SIMPLEX;
            e.setTransmissionType(type.getType());
        });
    }

    private MhaDbDto getMhaDbDto(Long mhaDbMappingId, Map<Long, MhaTblV2> mhaNameMap, Map<Long, DbTbl> dbNameMap, Map<Long, DcDo> dcDoMap) {
        MhaTblV2 mhaTblV2 = mhaNameMap.get(mhaDbMappingId);
        DbTbl dbTbl = dbNameMap.get(mhaDbMappingId);
        DcDo dcDo = dcDoMap.get(mhaTblV2.getDcId());
        return MhaDbDto.from(mhaDbMappingId, mhaTblV2, dbTbl, dcDo);
    }

    @Override
    public List<MhaDbDelayInfoDto> getReplicationDelays(List<Long> replicationIds) {
        try {
            List<MhaDbReplicationTbl> mhaDbReplicationTbls = mhaDbReplicationTblDao.queryByIds(replicationIds);
            List<MhaDbReplicationDto> replicationDtos = this.convert(mhaDbReplicationTbls);
            Map<Pair<String, String>, List<MhaDbReplicationDto>> map = replicationDtos.stream().collect(Collectors.groupingBy(e -> Pair.of(e.getSrc().getMhaName(), e.getDst().getMhaName())));
            List<MhaDbDelayInfoDto> res = Lists.newArrayList();
            for (Map.Entry<Pair<String, String>, List<MhaDbReplicationDto>> entry : map.entrySet()) {
                Pair<String, String> key = entry.getKey();
                List<MhaDbReplicationDto> value = entry.getValue();
                List<String> dbs = value.stream().map(e -> e.getSrc().getDbName()).distinct().collect(Collectors.toList());
                res.addAll(this.getMhaDbReplicationDelay(key.getKey(), key.getValue(), dbs));
            }
            return res;
        } catch (SQLException e) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
    }

    @Override
    public List<MhaDbReplicationDto> queryByDbNames(List<String> dbNames, ReplicationTypeEnum typeEnum) {
        try {
            List<DbTbl> dbTbls = dbTblDao.queryByDbNames(dbNames);
            List<Long> dbIds = dbTbls.stream().map(DbTbl::getId).distinct().collect(Collectors.toList());
            List<MhaDbMappingTbl> mhaDbMappingTbls = mhaDbMappingTblDao.queryByDbIds(dbIds);

            List<Long> relatedMappingTbls = mhaDbMappingTbls.stream().map(MhaDbMappingTbl::getId).collect(Collectors.toList());
            if (CollectionUtils.isEmpty(relatedMappingTbls)) {
                return Collections.emptyList();
            }

            // 2 query dbReplications/mhaDbReplication by (src -> dst) mapping id
            MhaDbReplicationQuery mhaDbReplicationQuery = new MhaDbReplicationQuery();
            mhaDbReplicationQuery.setRelatedMappingList(relatedMappingTbls);
            mhaDbReplicationQuery.setType(typeEnum.getType());
            List<MhaDbReplicationTbl> replicationTbls = mhaDbReplicationTblDao.query(mhaDbReplicationQuery);

            return convert(replicationTbls);
        } catch (SQLException e) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
    }

    /**
     * @return (failsafe) empty list if query fail
     */
    public List<MhaDbDelayInfoDto> getMhaDbReplicationDelay(String srcMha, String dstMha, List<String> dbNames) {

        // query dst first (result could be larger than querying src first)
        Long currentTime = mysqlServiceV2.getCurrentTime(srcMha);
        Map<String, Long> dstTimeMap = mysqlServiceV2.getDbDelayUpdateTime(srcMha, dstMha, dbNames);
        Map<String, Long> srcTimeMap = mysqlServiceV2.getDbDelayUpdateTime(srcMha, srcMha, dbNames);
        if (currentTime == null || dstTimeMap == null || srcTimeMap == null) {
            return Collections.emptyList();
        }

        return dbNames.stream().map(e -> {
            Long srcTime = srcTimeMap.get(e);
            Long dstTime = dstTimeMap.get(e);
            if (currentTime != null && srcTime != null) {
                srcTime = Math.max(srcTime, currentTime);
            } else {
                srcTime = null;
            }
            MhaDbDelayInfoDto dto = new MhaDbDelayInfoDto();
            dto.setDbName(e);
            dto.setSrcMha(srcMha);
            dto.setDstMha(dstMha);
            dto.setSrcTime(srcTime);
            dto.setDstTime(dstTime);
            return dto;
        }).collect(Collectors.toList());
    }

    /**
     * drc status: true with applier
     */
    private void fillDrcStatus(List<MhaDbReplicationDto> mhaDbReplicationDtos) throws SQLException {
        Map<Integer, List<MhaDbReplicationDto>> dtoByType = mhaDbReplicationDtos.stream().collect(Collectors.groupingBy(MhaDbReplicationDto::getReplicationType));

        // db to db
        List<MhaDbReplicationDto> dbToDbReplicationDtos = dtoByType.get(ReplicationTypeEnum.DB_TO_DB.getType());
        if (!CollectionUtils.isEmpty(dbToDbReplicationDtos)) {
            List<Long> ids = dbToDbReplicationDtos.stream().map(MhaDbReplicationDto::getId).collect(Collectors.toList());
            List<ApplierGroupTblV3> applierGroupTblV3s = applierGroupTblV3Dao.queryByMhaDbReplicationIds(ids);
            List<Long> groupIds = applierGroupTblV3s.stream().map(ApplierGroupTblV3::getId).collect(Collectors.toList());
            List<ApplierTblV3> applierTblV3s = applierTblV3Dao.queryByApplierGroupIds(groupIds, BooleanEnum.FALSE.getCode());
            Set<Long> existGroupIds = applierTblV3s.stream().map(ApplierTblV3::getApplierGroupId).collect(Collectors.toSet());
            Map<Long, Boolean> mhaReplicationIdToStatusMap = applierGroupTblV3s.stream().collect(Collectors.toMap(
                    ApplierGroupTblV3::getMhaDbReplicationId,
                    e -> existGroupIds.contains(e.getId())
            ));
            dbToDbReplicationDtos.forEach(e -> e.setDrcStatus(mhaReplicationIdToStatusMap.get(e.getId())));
        }

        // db to mq
        List<MhaDbReplicationDto> dbToMqReplicationDtos = dtoByType.get(ReplicationTypeEnum.DB_TO_MQ.getType());
        if (!CollectionUtils.isEmpty(dbToMqReplicationDtos)) {
            List<Long> ids = dbToMqReplicationDtos.stream().map(MhaDbReplicationDto::getId).collect(Collectors.toList());
            List<MessengerGroupTblV3> messengerGroupTblV3s = messengerGroupTblV3Dao.queryByMhaDbReplicationIds(ids);
            List<Long> groupIds = messengerGroupTblV3s.stream().map(MessengerGroupTblV3::getId).collect(Collectors.toList());
            List<MessengerTblV3> messengerTblV3s = messengerTblV3Dao.queryByGroupIds(groupIds);
            Set<Long> existGroupIds = messengerTblV3s.stream().map(MessengerTblV3::getMessengerGroupId).collect(Collectors.toSet());
            Map<Long, Boolean> mhaReplicationIdToStatusMap = messengerGroupTblV3s.stream().collect(Collectors.toMap(
                    MessengerGroupTblV3::getMhaDbReplicationId,
                    e -> existGroupIds.contains(e.getId())
            ));
            dbToMqReplicationDtos.forEach(e -> e.setDrcStatus(mhaReplicationIdToStatusMap.get(e.getId())));
        }
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public void refreshMhaReplication() {
        try {
            List<DbReplicationTbl> dbReplicationTbls = dbReplicationTblDao.queryAllExist();
            this.maintainMhaDbReplication(dbReplicationTbls);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public void maintainMhaDbReplication(List<DbReplicationTbl> dbReplicationTbls) throws SQLException {
        if (CollectionUtils.isEmpty(dbReplicationTbls)) {
            return;
        }
        dbReplicationTbls = this.filterGreyMha(dbReplicationTbls);
        this.insertAndUpdate(this.getInsertsAndUpdates(dbReplicationTbls));
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public void maintainMhaDbReplication(String srcMhaName, String dstMhaName, List<String> dbNames) throws SQLException {
        MhaTblV2 srcMhaTbl = mhaTblV2Dao.queryByMhaName(srcMhaName, 0);
        MhaTblV2 dstMhaTbl = mhaTblV2Dao.queryByMhaName(dstMhaName, 0);
        if (srcMhaTbl == null || dstMhaTbl == null) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.REQUEST_PARAM_INVALID, "mha not exist");
        }
        // 1. mha db mapping
        org.apache.commons.lang3.tuple.Pair<List<MhaDbMappingTbl>, List<MhaDbMappingTbl>> pair = mhaDbMappingService.initMhaDbMappings(srcMhaTbl, dstMhaTbl, dbNames);

        // 2. mha db replication
        List<DbTbl> dbTbls = dbTblDao.queryByDbNames(dbNames);
        Map<Long, String> dbIdToDbName = dbTbls.stream().collect(Collectors.toMap(DbTbl::getId, DbTbl::getDbName));
        List<MhaDbMappingTbl> srcMappingTbl = pair.getLeft();
        Map<String, MhaDbMappingTbl> dbNameToSrcMappingTblMap = srcMappingTbl.stream().collect(Collectors.toMap(e -> dbIdToDbName.get(e.getDbId()), e -> e));
        List<MhaDbMappingTbl> dstMappingTbl = pair.getRight();
        Map<String, MhaDbMappingTbl> dbNameToDstMappingTblMap = dstMappingTbl.stream().collect(Collectors.toMap(e -> dbIdToDbName.get(e.getDbId()), e -> e));


        List<MhaDbReplicationTbl> samples = dbNames.stream().map(dbName -> {
            MhaDbMappingTbl srcMapping = dbNameToSrcMappingTblMap.get(dbName);
            MhaDbMappingTbl dstMapping = dbNameToDstMappingTblMap.get(dbName);

            MhaDbReplicationTbl mhaDbReplicationTbl = new MhaDbReplicationTbl();
            mhaDbReplicationTbl.setSrcMhaDbMappingId(srcMapping.getId());
            mhaDbReplicationTbl.setDstMhaDbMappingId(dstMapping.getId());
            mhaDbReplicationTbl.setReplicationType(ReplicationTypeEnum.DB_TO_DB.getType());
            return mhaDbReplicationTbl;
        }).collect(Collectors.toList());

        this.insertAndUpdate(this.getInsertsAndUpdatesBySample(samples));
    }

    @Override
    public void offlineMhaDbReplication(String mhaName, String mhaName1) {
        try {
            List<MhaDbReplicationTbl> mhaDbReplicationTbls = this.getMhaDbReplicationTbls(mhaName, mhaName1, null);
            mhaDbReplicationTbls.forEach(e -> e.setDeleted(BooleanEnum.TRUE.getCode()));
            mhaDbReplicationTblDao.batchUpdate(mhaDbReplicationTbls);
        } catch (SQLException e) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.DAO_TBL_EXCEPTION, e);
        }
    }

    private void insertAndUpdate(Pair<List<MhaDbReplicationTbl>, List<MhaDbReplicationTbl>> insertsAndUpdates) throws SQLException {
        List<MhaDbReplicationTbl> inserts = insertsAndUpdates.getKey();
        List<MhaDbReplicationTbl> updates = insertsAndUpdates.getValue();
        mhaDbReplicationTblDao.batchInsert(inserts);
        mhaDbReplicationTblDao.batchUpdate(updates);
        this.maintainDelayMonitorDbTable(inserts, updates);
    }

    @Override
    public boolean isDbReplicationExist(Long mhaId, List<String> dbs) throws SQLException {
        List<DbTbl> dbTbls = dbTblDao.queryByDbNames(dbs);
        if (CollectionUtils.isEmpty(dbTbls)) {
            return false;
        }
        List<MhaDbMappingTbl> mhaDbMappingTbls = mhaDbMappingTblDao.queryByDbIdsAndMhaIds(
                dbTbls.stream().map(DbTbl::getId).collect(Collectors.toList()), Lists.newArrayList(mhaId));
        if (CollectionUtils.isEmpty(mhaDbMappingTbls)) {
            return false;
        }
        List<DbReplicationTbl> dbReplicationTbls = dbReplicationTblDao.queryMappingIds(mhaDbMappingTbls.stream().map(MhaDbMappingTbl::getId)
                .collect(Collectors.toList()));
        return !CollectionUtils.isEmpty(dbReplicationTbls);
    }

    @Override
    public List<MhaTblV2> getReplicationRelatedMha(String db, String table) throws SQLException {
        List<DbTbl> dbTbls = dbTblDao.queryByDbNames(Lists.newArrayList(db));
        if (CollectionUtils.isEmpty(dbTbls)) {
            return Lists.newArrayList();
        }
        List<MhaDbMappingTbl> mhaDbMappings = mhaDbMappingTblDao.queryByDbIds(dbTbls.stream().map(DbTbl::getId).collect(Collectors.toList()));
        if (CollectionUtils.isEmpty(mhaDbMappings)) {
            return Lists.newArrayList();
        }
        List<DbReplicationTbl> dbReplicationTbls = dbReplicationTblDao.queryByRelatedMappingIds(
                mhaDbMappings.stream().map(MhaDbMappingTbl::getId).collect(Collectors.toList()),
                ReplicationTypeEnum.DB_TO_DB.getType());
        List<DbReplicationTbl> matchDbReplications = dbReplicationTbls.stream().filter(dbReplicationTbl ->
                        new AviatorRegexFilter(dbReplicationTbl.getSrcLogicTableName()).filter(table))
                .collect(Collectors.toList());
        Set<Long> mhaDbMappingIds = Sets.newHashSet();
        matchDbReplications.forEach(dbReplicationTbl -> {
            mhaDbMappingIds.add(dbReplicationTbl.getSrcMhaDbMappingId());
            mhaDbMappingIds.add(dbReplicationTbl.getDstMhaDbMappingId());
        });
        Set<Long> matchMhaIds = mhaDbMappings.stream()
                .filter(mhaDbMapping -> mhaDbMappingIds.contains(mhaDbMapping.getId()))
                .map(MhaDbMappingTbl::getMhaId)
                .collect(Collectors.toSet());
        return mhaTblV2Dao.queryByIds(Lists.newArrayList(matchMhaIds));
    }

    private List<DbReplicationTbl> filterGreyMha(List<DbReplicationTbl> dbReplicationTbls) throws SQLException {
        List<Long> mappingIds = dbReplicationTbls.stream().map(e -> {
            if (ReplicationTypeEnum.DB_TO_MQ.getType().equals(e.getReplicationType())) {
                return e.getSrcMhaDbMappingId();
            } else {
                return e.getDstMhaDbMappingId();
            }
        }).collect(Collectors.toList());
        List<MhaDbMappingTbl> mhaDbMappingTbls = mhaDbMappingTblDao.queryByIds(mappingIds);
        List<Long> mhaIds = mhaDbMappingTbls.stream().map(MhaDbMappingTbl::getMhaId).collect(Collectors.toList());
        List<MhaTblV2> mhaTblV2List = mhaTblV2Dao.queryByIds(mhaIds);


        Map<Long, Boolean> mha = mhaTblV2List.stream()
                .collect(Collectors.toMap(MhaTblV2::getId, e -> defaultConsoleConfig.getDbApplierConfigureSwitch(e.getMhaName())));
        Set<Long> mappingId = mhaDbMappingTbls.stream().filter(e -> mha.get(e.getMhaId())).map(MhaDbMappingTbl::getId).collect(Collectors.toSet());

        return dbReplicationTbls.stream().filter(e -> {
            if (ReplicationTypeEnum.DB_TO_MQ.getType().equals(e.getReplicationType())) {
                return mappingId.contains(e.getSrcMhaDbMappingId());
            } else {
                return mappingId.contains(e.getDstMhaDbMappingId());
            }
        }).collect(Collectors.toList());
    }

    /**
     * create delay drc delay monitor table for dbs
     */
    private void maintainDelayMonitorDbTable(List<MhaDbReplicationTbl> inserts, List<MhaDbReplicationTbl> updates) throws SQLException {
        // 1. prepare data
        List<Long> mappingIds = Stream.concat(inserts.stream(), updates.stream())
                .filter(e -> BooleanEnum.FALSE.getCode().equals(e.getDeleted()))
                .flatMap(e -> Stream.of(e.getSrcMhaDbMappingId(), e.getDstMhaDbMappingId()))
                .filter(NumberUtils::isPositive)
                .distinct().collect(Collectors.toList());

        List<MhaDbMappingTbl> mhaDbMappingTbls = mhaDbMappingTblDao.queryByIds(mappingIds);
        List<Long> dbIds = mhaDbMappingTbls.stream().map(MhaDbMappingTbl::getDbId).distinct().collect(Collectors.toList());
        List<Long> mhaIds = mhaDbMappingTbls.stream().map(MhaDbMappingTbl::getMhaId).distinct().collect(Collectors.toList());
        Map<Long, String> dbMap = dbTblDao.queryByIds(dbIds).stream().collect(Collectors.toMap(DbTbl::getId, DbTbl::getDbName));
        Map<Long, String> mhaMap = mhaTblV2Dao.queryByIds(mhaIds).stream().collect(Collectors.toMap(MhaTblV2::getId, MhaTblV2::getMhaName));

        // src mha: [db1, db2] ; dst mha: [db1, db2]
        Map<String, List<String>> mhaToDbMap = mhaDbMappingTbls.stream().collect(
                Collectors.groupingBy(
                        e -> Objects.requireNonNull(mhaMap.get(e.getMhaId())),
                        Collectors.mapping(e -> Objects.requireNonNull(dbMap.get(e.getDbId())), Collectors.toList())
                )
        );

        // create table
        for (Map.Entry<String, List<String>> entry : mhaToDbMap.entrySet()) {
            String mhaName = entry.getKey();
            List<String> dbs = entry.getValue();
            Boolean createTableResult = mysqlServiceV2.createDrcMonitorDbTable(new DrcDbMonitorTableCreateReq(mhaName, dbs));
            if (!Boolean.TRUE.equals(createTableResult)) {
                throw new SQLException(String.format("create table error. mha: %s, dbs: %s", mhaName, dbs));
            }
        }
    }

    @VisibleForTesting
    protected Pair<List<MhaDbReplicationTbl>, List<MhaDbReplicationTbl>> getInsertsAndUpdates(List<DbReplicationTbl> dbReplicationTbls) throws SQLException {
        List<MhaDbReplicationTbl> samples = dbReplicationTbls.stream().map(e -> {
            MhaDbReplicationTbl mhaDbReplicationTbl = new MhaDbReplicationTbl();
            mhaDbReplicationTbl.setSrcMhaDbMappingId(e.getSrcMhaDbMappingId());
            mhaDbReplicationTbl.setDstMhaDbMappingId(e.getDstMhaDbMappingId());
            mhaDbReplicationTbl.setReplicationType(e.getReplicationType());
            return mhaDbReplicationTbl;
        }).filter(StreamUtils.distinctByKey(StreamUtils::getKey)).collect(Collectors.toList());
        return getInsertsAndUpdatesBySample(samples);
    }

    private Pair<List<MhaDbReplicationTbl>, List<MhaDbReplicationTbl>> getInsertsAndUpdatesBySample(List<MhaDbReplicationTbl> samples) throws SQLException {
        List<MhaDbReplicationTbl> mhaDbReplicationTbls = mhaDbReplicationTblDao.queryBySamples(samples);

        Map<MultiKey, MhaDbReplicationTbl> table = mhaDbReplicationTbls.stream().collect(Collectors.toMap(
                StreamUtils::getKey,
                e -> e
        ));


        // insert: in dbReplication, not in mhaReplication
        List<MhaDbReplicationTbl> insertTables = samples.stream().filter(e -> !table.containsKey(getKey(e))).map(e -> {
            MhaDbReplicationTbl mhaDbReplicationTbl = new MhaDbReplicationTbl();
            mhaDbReplicationTbl.setSrcMhaDbMappingId(e.getSrcMhaDbMappingId());
            mhaDbReplicationTbl.setDstMhaDbMappingId(e.getDstMhaDbMappingId());
            mhaDbReplicationTbl.setReplicationType(e.getReplicationType());
            mhaDbReplicationTbl.setDeleted(BooleanEnum.FALSE.getCode());
            return mhaDbReplicationTbl;
        }).filter(StreamUtils.distinctByKey(StreamUtils::getKey)).collect(Collectors.toList());

        // recover: in dbReplication, in mhaReplication but deleted
        List<MhaDbReplicationTbl> updateTables = samples.stream().map(e -> {
            MhaDbReplicationTbl mhaDbReplicationTbl = table.get(getKey(e));
            if (mhaDbReplicationTbl == null || Objects.equals(mhaDbReplicationTbl.getDeleted(), BooleanEnum.FALSE.getCode())) {
                return null;
            }
            mhaDbReplicationTbl.setDeleted(BooleanEnum.FALSE.getCode());
            return mhaDbReplicationTbl;
        }).filter(Objects::nonNull).collect(Collectors.toList());
        return Pair.from(insertTables, updateTables);
    }
}
