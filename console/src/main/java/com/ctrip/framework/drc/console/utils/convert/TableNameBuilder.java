package com.ctrip.framework.drc.console.utils.convert;

import com.ctrip.framework.drc.console.dao.entity.v2.DbReplicationTbl;
import com.google.common.base.Joiner;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TableNameBuilder {
    public static String buildNameFilter(Map<Long, String> srcDbTblMap, Map<Long, Long> srcMhaDbMappingMap, List<DbReplicationTbl> dbReplicationTblList) {
        List<String> nameFilterList = new ArrayList<>();
        for (DbReplicationTbl dbReplicationTbl : dbReplicationTblList) {
            long dbId = srcMhaDbMappingMap.getOrDefault(dbReplicationTbl.getSrcMhaDbMappingId(), 0L);
            if (srcDbTblMap.containsKey(dbId)) {
                nameFilterList.add(srcDbTblMap.getOrDefault(dbId, "") + "\\." + dbReplicationTbl.getSrcLogicTableName());
            }
        }
        return Joiner.on(",").join(nameFilterList);
    }

    public static String buildNameMapping(Map<Long, String> srcDbTblMap,
                                    Map<Long, Long> srcMhaDbMappingMap,
                                    Map<Long, String> dstDbTblMap,
                                    Map<Long, Long> dstMhaDbMappingMap,
                                    List<DbReplicationTbl> dbReplicationTblList) {
        List<String> nameMappingList = new ArrayList<>();
        for (DbReplicationTbl dbReplicationTbl : dbReplicationTblList) {
            if (StringUtils.isBlank(dbReplicationTbl.getDstLogicTableName())) {
                continue;
            }
            long srcDbId = srcMhaDbMappingMap.getOrDefault(dbReplicationTbl.getSrcMhaDbMappingId(), 0L);
            long dstDbId = dstMhaDbMappingMap.getOrDefault(dbReplicationTbl.getDstMhaDbMappingId(), 0L);
            String srcDbName = srcDbTblMap.getOrDefault(srcDbId, "");
            String dstDbName = dstDbTblMap.getOrDefault(dstDbId, "");
            nameMappingList.add(srcDbName + "." + dbReplicationTbl.getSrcLogicTableName() + "," + dstDbName + "." + dbReplicationTbl.getDstLogicTableName());
        }
        if (CollectionUtils.isEmpty(nameMappingList)) {
            return null;
        }
        return Joiner.on(";").join(nameMappingList);
    }
}
