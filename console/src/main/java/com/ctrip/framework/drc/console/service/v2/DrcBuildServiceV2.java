package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.dao.entity.ResourceTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.ApplierGroupTblV2;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaReplicationTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dto.MessengerMetaDto;
import com.ctrip.framework.drc.console.dto.v2.MachineDto;
import com.ctrip.framework.drc.console.param.v2.*;
import com.ctrip.framework.drc.console.vo.v2.ColumnsConfigView;
import com.ctrip.framework.drc.console.vo.v2.DbReplicationView;
import com.ctrip.framework.drc.console.vo.v2.RowsFilterConfigView;
import org.apache.commons.lang3.tuple.Pair;

import java.sql.SQLException;
import java.util.List;

/**
 * Created by dengquanliang
 * 2023/7/27 14:50
 */
public interface DrcBuildServiceV2 {

    // auto build machineDto should not empty,will change pwd;Manual without machineDto use default acc
    Pair<Long,Long> mhaInitBeforeBuildIfNeed(DrcMhaBuildParam param) throws Exception;

    // auto build machineDto should not empty,will change pwd;Manual without machineDto use default acc
    void buildMhaAndReplication(DrcMhaBuildParam param) throws Exception;

    // auto build machineDto should not empty,will change pwd;Manual without machineDto use default acc
    void buildMessengerMha(MessengerMhaBuildParam param) throws Exception;

    String buildDrc(DrcBuildParam param) throws Exception;

    void buildDbReplicationConfig(DbReplicationBuildParam param) throws Exception;

    Pair<Long, Long> checkDbReplicationFilter(List<Long> dbReplicationIds) throws Exception;

    List<Long> configureDbReplications(DbReplicationBuildParam param) throws Exception;

    List<DbReplicationView> getDbReplicationView(String srcMhaName, String dstMhaName) throws Exception;

    void deleteDbReplications(List<Long> dbReplicationIds) throws Exception;

    void buildColumnsFilter(ColumnsFilterCreateParam param) throws Exception;

    ColumnsConfigView getColumnsConfigView(long dbReplicationId) throws Exception;

    void deleteColumnsFilter(List<Long> dbReplicationIds) throws Exception;

    RowsFilterConfigView getRowsConfigView(long dbReplicationId) throws Exception;

    void buildRowsFilter(RowsFilterCreateParam param) throws Exception;

    void deleteRowsFilter(List<Long> dbReplicationIds) throws Exception;

    List<String> getMhaAppliers(String srcMhaName, String dstMhaName) throws Exception;

    String getApplierGtid(String srcMhaName, String dstMhaName) throws Exception;

    String buildMessengerDrc(MessengerMetaDto dto) throws Exception;

    // if oldMha not Blank ,copy oldMha accountInfo to newMha;otherwise init newMha and change AccountPwd
    MhaTblV2 syncMhaInfoFormDbaApi(String newMha, String oldMha) throws SQLException;
    
    void syncMhaDbInfoFromDbaApiIfNeeded(MhaTblV2 existMha, List<MachineDto> machineDtos) throws Exception;

    void autoConfigReplicatorsWithRealTimeGtid(MhaTblV2 mhaTbl) throws SQLException;

    void autoConfigReplicatorsWithGtid(MhaTblV2 mhaTbl, String gtidInit) throws SQLException;

    // switchOnly: true: only switch,not add when replication is empty; false: switch or add
    void autoConfigAppliers(MhaTblV2 srcMhaTbl, MhaTblV2 destMhaTbl, String gtid, boolean switchOnly) throws SQLException;
    
    void autoConfigAppliersWithRealTimeGtid(MhaReplicationTbl mhaReplicationTbl, ApplierGroupTblV2 applierGroup, MhaTblV2 srcMhaTbl, MhaTblV2 destMhaTbl) throws SQLException;

    // switchOnly: true: only switch,not add when replication is empty; false: switch or add
    void autoConfigAppliers(MhaReplicationTbl mhaReplicationTbl, ApplierGroupTblV2 applierGroup, MhaTblV2 srcMhaTbl,
            MhaTblV2 destMhaTbl, String mhaExecutedGtid, boolean switchOnly) throws SQLException;
    
    void autoConfigMessenger(MhaTblV2 srcMhaTbl, String gtid,boolean switchOnly) throws SQLException;

    void autoConfigMessengersWithRealTimeGtid(MhaTblV2 mhaTbl,boolean switchOnly) throws SQLException;

    void initReplicationTables() throws Exception;

    void deleteAllReplicationTables() throws Exception;

    Long configureReplicatorGroup(MhaTblV2 mhaTblV2, String replicatorInitGtid, List<String> replicatorIps, List<ResourceTbl> resourceTbls) throws Exception;

    String configReplicatorOnly(MessengerMetaDto dto) throws Exception;
    
    // return affect replication count
    int compensateGtidGap(GtidCompensateParam gtidCompensateParam) throws SQLException;
    
    int isolationMigrateReplicator(List<String> mhas, boolean master,String tag,String gtid) throws SQLException;
    
    int isolationMigrateApplier(List<String> mhas, String tag) throws  Exception;
    
    Pair<Boolean,String> checkIsoMigrateStatus(List<String> mhas,String tag) throws SQLException;
    
}
