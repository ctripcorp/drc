package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.DbTblDao;
import com.ctrip.framework.drc.console.dao.entity.DbTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaDbMappingTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.MhaDbMappingTblDao;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.ReadableErrorDefEnum;
import com.ctrip.framework.drc.console.param.v2.MhaQuery;
import com.ctrip.framework.drc.console.pojo.domain.DcDo;
import com.ctrip.framework.drc.console.service.v2.MetaInfoServiceV2;
import com.ctrip.framework.drc.console.service.v2.MhaDbMappingService;
import com.ctrip.framework.drc.console.service.v2.MysqlServiceV2;
import com.ctrip.framework.drc.console.service.v2.external.dba.DbaApiService;
import com.ctrip.framework.drc.console.service.v2.external.dba.response.DbClusterInfoDto;
import com.ctrip.framework.drc.console.utils.CommonUtils;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.console.utils.NumberUtils;
import com.ctrip.framework.drc.console.vo.request.MhaDbQueryDto;
import com.ctrip.framework.drc.console.vo.v2.ConfigDbView;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

/**
 * Created by dengquanliang
 * 2023/7/31 16:49
 */
@Service
public class MhaDbMappingServiceImpl implements MhaDbMappingService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private MysqlServiceV2 mysqlServiceV2;
    @Autowired
    private DefaultConsoleConfig defaultConsoleConfig;
    @Autowired
    private DbTblDao dbTblDao;
    @Autowired
    private MhaDbMappingTblDao mhaDbMappingTblDao;
    @Autowired
    private MhaTblV2Dao mhaTblV2Dao;
    @Autowired
    private MetaInfoServiceV2 metaInfoServiceV2;
    @Autowired
    private DbaApiService dbaApiService;

    private static final String DRC = "drc";
    private static final String DAL_CLUSTER = "_dalcluster";

    @Override
    public Pair<List<String>, List<String>> initMhaDbMappings(MhaTblV2 srcMha, MhaTblV2 dstMha, String nameFilter) throws Exception {
        logger.info("initMhaDbMappings, srcMha: {}, dstMha: {}, nameFilter: {}", srcMha.getMhaName(), dstMha.getMhaName(), nameFilter);
        List<String> vpcMhaNames = defaultConsoleConfig.getVpcMhaNames();
        if (vpcMhaNames.contains(srcMha.getMhaName()) && vpcMhaNames.contains(dstMha.getMhaName())) {
            logger.info("srcMha: {}, dstMha: {} are vpcMha", srcMha.getMhaName(), dstMha.getMhaName());
            throw ConsoleExceptionUtils.message("not support srcMha and dstMha are both IBU_VPC");
        }
        if (vpcMhaNames.contains(srcMha.getMhaName()) || vpcMhaNames.contains(dstMha.getMhaName())) {
            return insertVpcMhaDbMappings(srcMha, dstMha, vpcMhaNames, nameFilter);
        } else {
            return insertMhaDbMappings(srcMha, dstMha, nameFilter);
        }
    }

    @Override
    public void buildMhaDbMappings(String mhaName, List<String> dbList) throws SQLException {
        MhaTblV2 mhaTbl = mhaTblV2Dao.queryByMhaName(mhaName, BooleanEnum.FALSE.getCode());
        insertDbs(dbList);

        //insertMhaDbMappings
        insertMhaDbMappings(mhaTbl.getId(), dbList);
    }

    private Pair<List<String>, List<String>> insertMhaDbMappings(MhaTblV2 srcMha, MhaTblV2 dstMha, String nameFilter) throws Exception{
        List<String> srcTableList = mysqlServiceV2.queryTablesWithNameFilter(srcMha.getMhaName(), nameFilter);
        List<String> dstTableList = mysqlServiceV2.queryTablesWithNameFilter(dstMha.getMhaName(), nameFilter);
        List<String> srcDbList = extractDbs(srcTableList);
        List<String> dstDbList = extractDbs(dstTableList);
        if (CollectionUtils.isEmpty(srcDbList)) {
            throw ConsoleExceptionUtils.message("db table not found for srcMha: "+srcMha.getMhaName());
        }
        if (CollectionUtils.isEmpty(dstDbList)) {
            throw ConsoleExceptionUtils.message("db table not found for dstMha: "+dstMha.getMhaName());
        }
        if (!CommonUtils.isSameList(srcDbList, dstDbList)) {
            logger.error("insertMhaDbMappings srcDb dstDb is not same, srcDbList: {}, dstDbList: {}", srcDbList, dstDbList);
            throw ConsoleExceptionUtils.message("srcMha dstMha contains different dbs");
        }
        insertDbs(srcDbList);

        //insertMhaDbMappings
        insertMhaDbMappings(srcMha.getId(), srcDbList);
        insertMhaDbMappings(dstMha.getId(), dstDbList);

        return Pair.of(srcDbList, srcTableList);
    }

    private Pair<List<String>, List<String>> insertVpcMhaDbMappings(MhaTblV2 srcMha, MhaTblV2 dstMha, List<String> vpcMhaNames, String nameFilter) throws Exception {
        String mhaName = vpcMhaNames.contains(srcMha.getMhaName()) ? dstMha.getMhaName() : srcMha.getMhaName();
        List<String> tableList = mysqlServiceV2.queryTablesWithNameFilter(mhaName, nameFilter);
        List<String> dbList = extractDbs(tableList);
        insertDbs(dbList);

        //insertMhaDbMappings
        insertMhaDbMappings(srcMha.getId(), dbList);
        insertMhaDbMappings(dstMha.getId(), dbList);

        return Pair.of(dbList, tableList);
    }

    private void insertDbs(List<String> dbList) throws SQLException {
        if (CollectionUtils.isEmpty(dbList)) {
            logger.warn("dbList is empty");
            return;
        }
        List<String> existDbList = dbTblDao.queryByDbNames(dbList).stream().map(DbTbl::getDbName).collect(Collectors.toList());
        List<String> insertDbList = dbList.stream().filter(e -> !existDbList.contains(e)).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(insertDbList)) {
            logger.info("dbList has already exist: {}", dbList);
            return;
        }
        List<DbTbl> insertTbls = insertDbList.stream().map(db -> {
            DbTbl dbTbl = new DbTbl();
            dbTbl.setDbName(db);
            dbTbl.setIsDrc(0);
            dbTbl.setBuName(DRC);
            dbTbl.setBuCode(DRC);
            dbTbl.setDbOwner(DRC);
            dbTbl.setDeleted(BooleanEnum.FALSE.getCode());
            return dbTbl;
        }).collect(Collectors.toList());

        logger.info("insert dbList: {}", insertDbList);
        dbTblDao.batchInsert(insertTbls);
    }

    private List<String> extractDbs(List<String> tableList) {
        List<String> dbList = new ArrayList<>();
        if (CollectionUtils.isEmpty(tableList)) {
            return dbList;
        }
        for (String table : tableList) {
            String[] tables = table.split("\\.");
            dbList.add(tables[0]);
        }
        return dbList.stream().distinct().collect(Collectors.toList());
    }

    private void insertMhaDbMappings(long mhaId, List<String> dbList) throws SQLException {
        if (CollectionUtils.isEmpty(dbList)) {
            logger.warn("dbList is empty, mhaId: {}", mhaId);
            return;
        }
        List<DbTbl> dbTblList = dbTblDao.queryByDbNames(dbList).stream().collect(Collectors.toList());
        Map<String, Long> dbMap = dbTblList.stream().collect(Collectors.toMap(DbTbl::getDbName, DbTbl::getId));
        List<Long> existDbIds = mhaDbMappingTblDao.queryByMhaId(mhaId).stream().map(MhaDbMappingTbl::getDbId).collect(Collectors.toList());

        List<MhaDbMappingTbl> insertDbMappingTbls = new ArrayList<>();
        for (String dbName : dbList) {
            long dbId = dbMap.get(dbName);
            if (existDbIds.contains(dbId)) {
                continue;
            }
            MhaDbMappingTbl mhaDbMappingTbl = new MhaDbMappingTbl();
            mhaDbMappingTbl.setMhaId(mhaId);
            mhaDbMappingTbl.setDbId(dbId);
            mhaDbMappingTbl.setDeleted(BooleanEnum.FALSE.getCode());
            insertDbMappingTbls.add(mhaDbMappingTbl);
        }

        if (CollectionUtils.isEmpty(insertDbMappingTbls)) {
            logger.info("mhaDbMappings has already exist, mhaId: {}, dbList: {}", mhaId, dbList);
            return;
        }

        logger.info("insertDbMappingTbls: {}", insertDbMappingTbls);
        mhaDbMappingTblDao.batchInsert(insertDbMappingTbls);
    }

    @Override
    public void copyAndInitMhaDbMappings(MhaTblV2 newMhaTbl, List<MhaDbMappingTbl> mhaDbMappingInOldMha) throws SQLException {
        List<MhaDbMappingTbl> exist = mhaDbMappingTblDao.queryByMhaId(newMhaTbl.getId());
        List<MhaDbMappingTbl> toBeInsert = Lists.newArrayList();
        Set<Long> existedDb = exist.stream().map(MhaDbMappingTbl::getDbId).collect(Collectors.toSet());
        List<MhaDbMappingTbl> copies = copyFrom(newMhaTbl, mhaDbMappingInOldMha);
        copies.forEach(mhaDbMappingTbl -> {
            if (!existedDb.contains(mhaDbMappingTbl.getDbId())) {
                toBeInsert.add(mhaDbMappingTbl);
            }
        });
        if (!CollectionUtils.isEmpty(toBeInsert)) {
            int[] ints = mhaDbMappingTblDao.batchInsert(toBeInsert);
            logger.info("copyMhaDbMappings from :{},expected size: {} ,res:{}",
                    newMhaTbl.getMhaName(), toBeInsert.size(), Arrays.stream(ints).sum());
        }
    }


    @Override
    public List<MhaDbMappingTbl> query(MhaDbQueryDto query) {
        try {
            // mha id
            List<Long> mhaIds = Lists.newArrayList();
            List<Long> dbIds = Lists.newArrayList();
            if (query.hasMhaCondition()) {
                MhaQuery mhaQuery = new MhaQuery();
                mhaQuery.setContainMhaName(query.getMhaName());
                if (NumberUtils.isPositive(query.getRegionId())) {
                    List<DcDo> dcDos = metaInfoServiceV2.queryAllDcWithCache();
                    List<Long> dcIdList = dcDos.stream()
                            .filter(e -> query.getRegionId().equals(e.getRegionId()))
                            .map(DcDo::getDcId)
                            .collect(Collectors.toList());
                    if (CollectionUtils.isEmpty(dcIdList)) {
                        return Collections.emptyList();
                    }
                    mhaQuery.setDcIdList(dcIdList);
                }

                if (mhaQuery.emptyQueryCondition()) {
                    return Collections.emptyList();
                }
                mhaIds = mhaTblV2Dao.query(mhaQuery)
                        .stream().map(MhaTblV2::getId).collect(Collectors.toList());
                if(CollectionUtils.isEmpty(mhaIds)){
                    return Collections.emptyList();
                }
            }
            if (query.hasDbCondition()) {
                dbIds = dbTblDao.queryByLikeDbNamesOrBuCode(query.getDbName(), query.getBuCode())
                        .stream().map(DbTbl::getId).collect(Collectors.toList());
                if(CollectionUtils.isEmpty(dbIds)){
                    return Collections.emptyList();
                }
            }
            return mhaDbMappingTblDao.queryByDbIdsOrMhaIds(dbIds, mhaIds);
        } catch (SQLException e) {
            logger.error("queryMhaByName exception", e);
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
    }

    @Override
    public Pair<Integer, Integer> removeDuplicateDbTblWithoutMhaDbMapping(boolean executeDelete) throws SQLException {
        List<DbTbl> dbTbls = dbTblDao.queryAll();
        List<DbTbl> dbTblsTobeDeleted = Lists.newArrayList();
        Map<String, List<DbTbl>> dbTblMap = dbTbls.stream().collect(Collectors.groupingBy(e -> e.getDbName().toLowerCase()));
        for (Entry<String, List<DbTbl>> entry : dbTblMap.entrySet()) {
            String dbName = entry.getKey();
            List<DbTbl> dbTblList = entry.getValue();
            if (dbTblList.size() > 1) {
                logger.info("duplicate dbName: {},dbTblList size: {},",dbName, dbTblList.size());
                dbTblList.sort(Comparator.comparing(DbTbl::getCreateTime));
                List<DbTbl> duplicateDbTbls = dbTblList.subList(1, dbTblList.size());
                List<MhaDbMappingTbl> mhaDbMappingTbls = mhaDbMappingTblDao.queryByDbIdsIgnoreDeleted(duplicateDbTbls.stream().map(DbTbl::getId).collect(Collectors.toList()));
                if (!CollectionUtils.isEmpty(mhaDbMappingTbls)) {
                    Iterator<DbTbl> iterator = duplicateDbTbls.iterator();
                    Set<Long> dbIdsWithMhaDbMapping = mhaDbMappingTbls.stream().map(MhaDbMappingTbl::getDbId).collect(Collectors.toSet());
                    while (iterator.hasNext()) {
                        DbTbl dbTbl = iterator.next();
                        if (dbIdsWithMhaDbMapping.contains(dbTbl.getId())) {
                            logger.warn("dbTbl duplicate,has mhaDbMapping not remove, dbTbl: {}", dbTbl);
                            iterator.remove();
                        }
                    }
                }
                if (!CollectionUtils.isEmpty(duplicateDbTbls)) {
                    dbTblsTobeDeleted.addAll(duplicateDbTbls);
                }
            }
        }
        if (!CollectionUtils.isEmpty(dbTblsTobeDeleted)) {
            logger.info("dbTblsTobeDeleted: {}", dbTblsTobeDeleted);
            if (executeDelete) {
                int batchSize = 100;
                int size = dbTblsTobeDeleted.size();
                int batchCount = size / batchSize + 1;
                int affectedRows = 0;
                for (int i = 0; i < batchCount; i++) {
                    int fromIndex = i * batchSize;
                    int toIndex = Math.min((i + 1) * batchSize, size);
                    List<DbTbl> subList = dbTblsTobeDeleted.subList(fromIndex, toIndex);
                    int[] ints = dbTblDao.batchDelete(subList);
                    logger.info("batchDelete dbTblsTobeDeleted, fromIndex: {}, toIndex: {}, res: {}", fromIndex, toIndex, Arrays.stream(ints).sum());
                    affectedRows += Arrays.stream(ints).sum();
                }
                return Pair.of(dbTblsTobeDeleted.size(), affectedRows);
            }
            return Pair.of(dbTblsTobeDeleted.size(), 0);
        }
        return Pair.of(0, 0);
    }

    @Override
    public ConfigDbView configEmailGroupForDb(String dalCluster, String emailGroup) throws Exception {
        List<String> dbNames;
        if (dalCluster.endsWith(DAL_CLUSTER)) {
            List<DbClusterInfoDto> list = dbaApiService.getDatabaseClusterInfoList(dalCluster);
            if (CollectionUtils.isEmpty(list)) {
                return new ConfigDbView();
            }
            dbNames = list.stream().map(DbClusterInfoDto::getDbName).distinct().collect(Collectors.toList());
        } else {
            dbNames = Lists.newArrayList(dalCluster);
        }

        List<DbTbl> dbTbls = dbTblDao.queryByDbNames(dbNames);
        dbTbls.stream().forEach(tbl -> tbl.setEmailGroup(emailGroup));
        dbTblDao.update(dbTbls);
        List<String> dbList = dbTbls.stream().map(DbTbl::getDbName).collect(Collectors.toList());
        return new ConfigDbView(dbList, dbList.size());
    }

    private List<MhaDbMappingTbl> copyFrom(MhaTblV2 newMhaTbl, List<MhaDbMappingTbl> mhaDbMappingInOldMha) {
        List<MhaDbMappingTbl> res = Lists.newArrayList();
        for (MhaDbMappingTbl mhaDbMappingTbl : mhaDbMappingInOldMha) {
            MhaDbMappingTbl copy = new MhaDbMappingTbl();
            copy.setMhaId(newMhaTbl.getId());
            copy.setDbId(mhaDbMappingTbl.getDbId());
            copy.setDeleted(BooleanEnum.FALSE.getCode());
            res.add(copy);
        }
        return res;
    }
}
