package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.dao.entity.v2.MhaDbMappingTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.vo.request.MhaDbQueryDto;
import com.ctrip.framework.drc.console.vo.v2.ConfigDbView;
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

    void buildMhaDbMappings(String mhaName,List<String> dbList) throws SQLException;

    Pair<List<MhaDbMappingTbl>, List<MhaDbMappingTbl>> initMhaDbMappings(MhaTblV2 srcMha, MhaTblV2 dstMha, List<String> dbNames) throws SQLException;
    List<MhaDbMappingTbl> initMhaDbMappings(MhaTblV2 srcMha, List<String> dbNames) throws SQLException;

    void copyAndInitMhaDbMappings(MhaTblV2 newMhaTbl, List<MhaDbMappingTbl> mhaDbMappingInOldMha) throws SQLException;

    List<MhaDbMappingTbl> query(MhaDbQueryDto mhaDbQueryDto);
    
    // tmp api
    Pair<Integer,Integer> removeDuplicateDbTblWithoutMhaDbMapping(boolean executeDelete) throws SQLException;

    ConfigDbView configEmailGroupForDb(String dalCluster, String emailGroup) throws Exception;
}
