package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.entity.v3.ApplierGroupTblV3;
import com.ctrip.framework.drc.console.dao.entity.v3.MhaDbReplicationTbl;
import com.ctrip.framework.drc.console.dto.v3.*;
import com.ctrip.framework.drc.console.param.v2.DrcBuildBaseParam;
import com.ctrip.framework.drc.console.param.v2.DrcBuildParam;
import com.ctrip.framework.drc.console.vo.v2.ColumnsConfigView;
import com.ctrip.framework.drc.console.vo.v2.RowsFilterConfigView;

import java.sql.SQLException;
import java.util.List;

public interface DbDrcBuildService {

    List<DbApplierDto> getMhaDbAppliers(String srcMhaName, String dstMhaName);

    void setMhaDbAppliers(List<MhaDbReplicationDto> replicationDtos);

    List<DbApplierDto> getMhaDbMessengers(String mhaName) throws Exception;

    void setMhaDbMessengers(List<MhaDbReplicationDto> replicationDtos) ;

    /**
     * @param param drc build param
     * @return drc xml
     */
    void buildDbApplier(DrcBuildParam param) throws Exception;

    String buildDbMessenger(DrcBuildBaseParam param) throws Exception;

    void switchAppliers(List<DbApplierSwitchReqDto> reqDtos) throws Exception;
    void switchMessengers(List<DbApplierSwitchReqDto> reqDtos) throws Exception;

    // switchOnly: true: only switch,not add when replication is empty; false: switch or add
    void autoConfigDbAppliers(String srcMha, String dstMha, List<String> dbNames, String initGtid,boolean switchOnly) throws Exception;

    void autoConfigDbAppliers(MhaDbReplicationTbl mhaDbReplication, ApplierGroupTblV3 applierGroup, MhaTblV2 drcMhaTbl ,MhaTblV2 destMhaTbl,
                              String dbExecutedGtid, Integer concurrency, boolean switchOnly) throws SQLException;

    void autoConfigDbAppliersWithRealTimeGtid(MhaDbReplicationTbl mhaDbReplication, ApplierGroupTblV3 applierGroup,
                                              MhaTblV2 srcMhaTbl, MhaTblV2 destMhaTbl, Integer concurrency) throws SQLException;

    List<DbDrcConfigInfoDto> getExistDbReplicationDirections(String dbName);
    List<DbMqConfigInfoDto> getExistDbMqConfigDcOption(String dbName);

    DbDrcConfigInfoDto getDbDrcConfig(String dbName, String srcRegionName, String dstRegionName);

    DbMqConfigInfoDto getDbMqConfig(String dbName, String srcRegionName);

    RowsFilterConfigView getRowsConfigViewById(long rowsFilterId);

    ColumnsConfigView getColumnsConfigViewById(long colsFilterId);

    void createDbReplication(DbReplicationCreateDto createDto) throws Exception;

    void createMhaDbDrcReplication(MhaDbReplicationCreateDto createDto) throws Exception;

    void createMhaDbReplicationForMq(MhaDbReplicationCreateDto createDto) throws Exception;

    void editDbReplication(DbReplicationEditDto editDto) throws Exception;

    void deleteDbReplication(DbReplicationEditDto editDto) throws Exception;

    void createDbMqReplication(DbMqCreateDto dbMqCreateDto) throws Exception;

    void editDbMqReplication(DbMqEditDto editDto) throws Exception;

    void deleteDbMqReplication(DbMqEditDto editDto) throws Exception;

}
