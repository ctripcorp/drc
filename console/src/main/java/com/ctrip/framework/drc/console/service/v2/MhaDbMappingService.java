package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import org.apache.commons.lang3.tuple.Pair;

import java.sql.SQLException;
import java.util.List;

/**
 * Created by dengquanliang
 * 2023/7/31 16:48
 */
public interface MhaDbMappingService {
    /**
     * resultPair
     * left: dbNames right: fullTables exp: db.table
     */
    Pair<List<String>, List<String>> initMhaDbMappings(MhaTblV2 srcMha, MhaTblV2 dstMha, String nameFilter) throws Exception;

    void buildMhaDbMappings(String mhaName, List<String> dbList) throws SQLException;
}
