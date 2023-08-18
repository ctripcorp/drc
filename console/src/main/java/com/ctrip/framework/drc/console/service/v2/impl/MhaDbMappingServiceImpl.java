package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.DbTblDao;
import com.ctrip.framework.drc.console.dao.entity.DbTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaDbMappingTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.MhaDbMappingTblDao;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.service.v2.MhaDbMappingService;
import com.ctrip.framework.drc.console.service.v2.MysqlServiceV2;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.core.server.common.filter.table.aviator.AviatorRegexFilter;
import com.ctrip.framework.drc.core.service.utils.Constants;
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

    private static final String DRC = "drc";

    @Override
    public List<String> buildMhaDbMappings(MhaTblV2 srcMha, MhaTblV2 dstMha, String nameFilter) throws Exception {
        logger.info("buildMhaDbMappings, srcMha: {}, dstMha: {}, nameFilter: {}", srcMha.getMhaName(), dstMha.getMhaName(), nameFilter);
        List<String> vpcMhaNames = defaultConsoleConfig.getVpcMhaNames();
        if (vpcMhaNames.contains(srcMha.getMhaName()) && vpcMhaNames.contains(dstMha.getMhaName())) {
            logger.info("srcMha: {}, dstMha: {} are vpcMha", srcMha.getMhaName(), dstMha.getMhaName());
            return getVpcMhaDbs(srcMha, dstMha, nameFilter);
        }
        if (vpcMhaNames.contains(srcMha.getMhaName()) || vpcMhaNames.contains(dstMha.getMhaName())) {
            return insertVpcMhaDbMappings(srcMha, dstMha, vpcMhaNames, nameFilter);
        } else {
            return insertMhaDbMappings(srcMha, dstMha, nameFilter);
        }
    }

    private List<String> getVpcMhaDbs(MhaTblV2 srcMha, MhaTblV2 dstMha, String nameFilter) throws Exception {
        List<MhaDbMappingTbl> srcMhaDbMappings = mhaDbMappingTblDao.queryByMhaId(srcMha.getId());
        List<MhaDbMappingTbl> dstMhaDbMappings = mhaDbMappingTblDao.queryByMhaId(dstMha.getId());
        List<Long> srcDbIds = srcMhaDbMappings.stream().map(MhaDbMappingTbl::getDbId).collect(Collectors.toList());
        List<Long> dstDbIds = dstMhaDbMappings.stream().map(MhaDbMappingTbl::getDbId).collect(Collectors.toList());

        String dbFilter = nameFilter.split(Constants.ESCAPE_CHARACTER_DOT_REGEX)[0];
        AviatorRegexFilter aviatorRegexFilter = new AviatorRegexFilter(dbFilter);
        List<String> srcDbList = dbTblDao.queryByIds(srcDbIds).stream()
                .map(DbTbl::getDbName)
                .filter(e -> aviatorRegexFilter.filter(e))
                .collect(Collectors.toList());
        List<String> dstDbList = dbTblDao.queryByIds(dstDbIds).stream().map(DbTbl::getDbName)
                .filter(e -> aviatorRegexFilter.filter(e))
                .collect(Collectors.toList());

        if (!checkDbIsSame(srcDbList, dstDbList)) {
            logger.error("srcMha: {} and dstMha: {} contain different db, srcDbList: {}, dstDbList: {}", srcMha.getMhaName(), dstMha.getMhaName(), srcDbList, dstDbList);
            throw ConsoleExceptionUtils.message("srcMha and dstMha contain different db");
        }
        return srcDbList;
    }

    private List<String> insertMhaDbMappings(MhaTblV2 srcMha, MhaTblV2 dstMha, String nameFilter) throws Exception{
        List<String> srcDbList = queryDbs(srcMha.getMhaName(), nameFilter);
        List<String> dstDbList = queryDbs(dstMha.getMhaName(), nameFilter);
        if (!checkDbIsSame(srcDbList, dstDbList)) {
            logger.error("insertMhaDbMappings srcDb dstDb is not same, srcDbList: {}, dstDbList: {}", srcDbList, dstDbList);
            throw ConsoleExceptionUtils.message("srcMha dstMha contains different dbs");
        }
        insertDbs(srcDbList);

        //insertMhaDbMappings
        insertMhaDbMappings(srcMha.getId(), srcDbList);
        insertMhaDbMappings(dstMha.getId(), dstDbList);

        return srcDbList;
    }

    private boolean checkDbIsSame(List<String> srcDbList, List<String> dstDbList) {
        if (CollectionUtils.isEmpty(srcDbList) || CollectionUtils.isEmpty(dstDbList)) {
            return false;
        }
        if (srcDbList.size() != dstDbList.size()) {
            return false;
        }
        Collections.sort(srcDbList);
        Collections.sort(dstDbList);
        return srcDbList.equals(dstDbList);
    }


    private List<String> insertVpcMhaDbMappings(MhaTblV2 srcMha, MhaTblV2 dstMha, List<String> vpcMhaNames, String nameFilter) throws Exception {
        String mhaName = vpcMhaNames.contains(srcMha.getMhaName()) ? dstMha.getMhaName() : srcMha.getMhaName();
        List<String> dbList = queryDbs(mhaName, nameFilter);
        insertDbs(dbList);

        //insertMhaDbMappings
        insertMhaDbMappings(srcMha.getId(), dbList);
        insertMhaDbMappings(dstMha.getId(), dbList);

        return dbList;
    }

    private void insertDbs(List<String> dbList) throws Exception {
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

    private List<String> queryDbs(String mhaName, String nameFilter) {
        List<String> tableList = mysqlServiceV2.queryTablesWithNameFilter(mhaName, nameFilter);
        List<String> dbList = new ArrayList<>();
        if (CollectionUtils.isEmpty(tableList)) {
            logger.info("mha: {} query db empty, nameFilter: {}", mhaName, nameFilter);
            return dbList;
        }
        for (String table : tableList) {
            String[] tables = table.split("\\.");
            dbList.add(tables[0]);
        }
        return dbList.stream().distinct().collect(Collectors.toList());
    }

    private void insertMhaDbMappings(long mhaId, List<String> dbList) throws Exception {
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
