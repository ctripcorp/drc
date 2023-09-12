package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.DbTblDao;
import com.ctrip.framework.drc.console.dao.entity.DbTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaDbMappingTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.MhaDbMappingTblDao;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.service.v2.MhaDbMappingService;
import com.ctrip.framework.drc.console.service.v2.MysqlServiceV2;
import com.ctrip.framework.drc.console.utils.CommonUtils;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.core.server.common.filter.table.aviator.AviatorRegexFilter;
import com.ctrip.framework.drc.core.service.utils.Constants;
import java.sql.SQLException;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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

    private static final String DRC = "drc";

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
    public void buildMhaDbMappings(String mhaName,List<String> dbList) throws SQLException {
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
}
