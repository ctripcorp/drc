package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.dao.entity.ColumnsFilterTbl;
import com.ctrip.framework.drc.console.dao.entity.DbTbl;
import com.ctrip.framework.drc.console.dao.entity.MachineTbl;
import com.ctrip.framework.drc.console.dao.entity.MessengerGroupTbl;
import com.ctrip.framework.drc.console.dao.entity.MessengerTbl;
import com.ctrip.framework.drc.console.dao.entity.ReplicatorGroupTbl;
import com.ctrip.framework.drc.console.dao.entity.ReplicatorTbl;
import com.ctrip.framework.drc.console.dao.entity.ResourceTbl;
import com.ctrip.framework.drc.console.dao.entity.RowsFilterTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.ApplierGroupTblV2;
import com.ctrip.framework.drc.console.dao.entity.v2.DbReplicationFilterMappingTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.DbReplicationTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MessengerFilterTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaDbMappingTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaReplicationTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.entity.v2.MigrationTaskTbl;
import com.ctrip.framework.drc.console.monitor.consistency.cases.Row;
import com.ctrip.framework.drc.console.vo.v2.ResourceView;
import com.google.common.collect.Lists;
import java.util.List;

/**
 * @ClassName MockEntityBuilder
 * @Author haodongPan
 * @Date 2023/8/1 11:28
 * @Version: $
 */
public class MockEntityBuilder {
    
    public static MhaTblV2 buildMhaTblV2() {
        MhaTblV2 tbl = new MhaTblV2();
        tbl.setId(1L);
        tbl.setMhaName("mha");
        tbl.setDcId(1L);
        tbl.setBuId(1L);
        tbl.setClusterName("cluster");
        tbl.setReadUser("readUser");
        tbl.setReadPassword("readPsw");
        tbl.setWriteUser("writeUser");
        tbl.setWritePassword("writePsw");
        tbl.setMonitorUser("monitorUser");
        tbl.setMonitorPassword("monitorPsw");
        tbl.setDeleted(0);
        tbl.setAppId(1L);
        tbl.setApplyMode(1);
        return tbl;
    }

    public static MhaTblV2 buildMhaTblV2(Long id,String mhaName,Long dcId) {
        MhaTblV2 tbl = new MhaTblV2();
        tbl.setId(id);
        tbl.setMhaName(mhaName);
        tbl.setDcId(dcId);
        tbl.setBuId(1L);
        tbl.setClusterName("cluster");
        tbl.setReadUser("readUser");
        tbl.setReadPassword("readPsw");
        tbl.setWriteUser("writeUser");
        tbl.setWritePassword("writePsw");
        tbl.setMonitorUser("monitorUser");
        tbl.setMonitorPassword("monitorPsw");
        tbl.setDeleted(0);
        tbl.setAppId(1L);
        tbl.setApplyMode(1);
        return tbl;
    }

    public static ReplicatorGroupTbl buildReplicatorGroupTbl() {
        ReplicatorGroupTbl replicatorGroupTbl = new ReplicatorGroupTbl();
        replicatorGroupTbl.setId(1L);
        replicatorGroupTbl.setMhaId(1L);
        replicatorGroupTbl.setDeleted(0);
        return replicatorGroupTbl;
    }

    public static ReplicatorGroupTbl buildReplicatorGroupTbl(Long pk, Long mhaId) {
        ReplicatorGroupTbl replicatorGroupTbl = new ReplicatorGroupTbl();
        replicatorGroupTbl.setId(1L);
        replicatorGroupTbl.setMhaId(1L);
        replicatorGroupTbl.setDeleted(0);
        return replicatorGroupTbl;
    }

    public static List<ReplicatorTbl> buildReplicatorTbls() {
        List<ReplicatorTbl> res = Lists.newArrayList();
        for (int i = 1; i < 3; i++) {
            ReplicatorTbl replicatorTbl = new ReplicatorTbl();
            if (i == 1) {
                replicatorTbl.setMaster(1);
            } else {
                replicatorTbl.setMaster(0);
            }
            replicatorTbl.setRelicatorGroupId(1L);
            replicatorTbl.setResourceId((long) i);
            res.add(replicatorTbl);
        }
        return res;
    }

    public static ResourceTbl buildResourceTbl() {
        ResourceTbl resourceTbl = new ResourceTbl();
        resourceTbl.setId(1L);
        resourceTbl.setIp("ip");
        return resourceTbl;
    }

    public static MachineTbl buildMachineTbl() {
        MachineTbl machineTbl = new MachineTbl();
        machineTbl.setMaster(1);
        machineTbl.setMhaId(1L);
        machineTbl.setIp("ip");
        machineTbl.setPort(3306);
        return machineTbl;
    }
    
    public static List<MachineTbl> buildMachineTbls() {
        List<MachineTbl> res = Lists.newArrayList();
        for (int i = 1; i < 4; i++) {
            MachineTbl machineTbl = new MachineTbl();
            if (i == 1) {
                machineTbl.setMaster(1);
            } else {
                machineTbl.setMaster(0);
            }
            machineTbl.setMhaId(1L);
            machineTbl.setIp("ip" + i);
            machineTbl.setPort(i);
            res.add(machineTbl);
        }
        return res;
    }

    public static List<DbTbl> buildDbTblList(int count) {
        List<DbTbl> res = Lists.newArrayList();
        for (int i = 1; i < count + 1; i++) {
            DbTbl dbTbl = new DbTbl();
            dbTbl.setId((long) i);
            dbTbl.setDbName("db" + i);
            dbTbl.setDeleted(0);
            res.add(dbTbl);
        }
        return res;
    }
    
    
    public static DbTbl buildDbTbl(Long pk, String dbName) {
        DbTbl dbTbl = new DbTbl();
        dbTbl.setId(pk);
        dbTbl.setDbName(dbName);
        dbTbl.setDeleted(0);
        return dbTbl;
    }
    

    public static List<MhaDbMappingTbl> buildMhaDbMappings(int count,long mhaId) {
        List<MhaDbMappingTbl> res = Lists.newArrayList();
        for (int i = 1; i < count + 1; i++) {
            MhaDbMappingTbl mhaDbMappingTbl = new MhaDbMappingTbl();
            mhaDbMappingTbl.setId(Long.valueOf(i));
            mhaDbMappingTbl.setMhaId(Long.valueOf(mhaId));
            mhaDbMappingTbl.setDbId(Long.valueOf(i));
            mhaDbMappingTbl.setDeleted(0);
            res.add(mhaDbMappingTbl);
        }
        return res;
    }

    public static List<DbReplicationTbl> buildDbReplicationTblListBySrc(int count, long srcMappingId) {
        List<DbReplicationTbl> res = Lists.newArrayList();
        for (int i = 1; i < count + 1; i++) {
            DbReplicationTbl dbReplicationTbl = new DbReplicationTbl();
            dbReplicationTbl.setId(Long.valueOf(i));
            dbReplicationTbl.setSrcMhaDbMappingId(Long.valueOf(srcMappingId));
            dbReplicationTbl.setSrcLogicTableName("srcLogicTableName" + i);
            dbReplicationTbl.setDstMhaDbMappingId(Long.valueOf(i));
            dbReplicationTbl.setDstLogicTableName("dstLogicTableName" + i);
            dbReplicationTbl.setDeleted(0);
            res.add(dbReplicationTbl);
        }
        return res;
    }

    public static List<DbReplicationTbl> buildDbReplicationTblListByDest(int count, long destMappingId) {
        List<DbReplicationTbl> res = Lists.newArrayList();
        for (int i = 1; i < count + 1; i++) {
            DbReplicationTbl dbReplicationTbl = new DbReplicationTbl();
            dbReplicationTbl.setId(Long.valueOf(i));
            dbReplicationTbl.setSrcMhaDbMappingId(Long.valueOf(i));
            dbReplicationTbl.setSrcLogicTableName("srcLogicTableName" + i);
            dbReplicationTbl.setDstMhaDbMappingId(Long.valueOf(destMappingId));
            dbReplicationTbl.setDstLogicTableName("dstLogicTableName" + i);
            dbReplicationTbl.setDeleted(0);
            res.add(dbReplicationTbl);
        }
        return res;
    }

    public static MhaReplicationTbl buildMhaReplicationTbl(Long pk,MhaTblV2 mha1, MhaTblV2 mha2) {
        MhaReplicationTbl mhaReplicationTbl = new MhaReplicationTbl();
        mhaReplicationTbl.setId(1L);
        mhaReplicationTbl.setSrcMhaId(mha1.getId());
        mhaReplicationTbl.setDstMhaId(mha2.getId());
        mhaReplicationTbl.setDrcStatus(1);
        return mhaReplicationTbl;
    }

    public static MhaDbMappingTbl buildMhaDbMappingTbl(Long pk, MhaTblV2 mha1, DbTbl db1) {
        MhaDbMappingTbl mhaDbMappingTbl = new MhaDbMappingTbl();
        mhaDbMappingTbl.setId(pk);
        mhaDbMappingTbl.setMhaId(mha1.getId());
        mhaDbMappingTbl.setDbId(db1.getId());
        mhaDbMappingTbl.setDeleted(0);
        return mhaDbMappingTbl;
    }

    public static DbReplicationTbl buildDbReplicationTbl(long pk, MhaDbMappingTbl srcMap, MhaDbMappingTbl destMap, int type) {
        DbReplicationTbl dbReplicationTbl = new DbReplicationTbl();
        dbReplicationTbl.setId(pk);
        dbReplicationTbl.setSrcMhaDbMappingId(srcMap.getId());
        dbReplicationTbl.setSrcLogicTableName("srcLogicTableName" + pk);
        dbReplicationTbl.setDstMhaDbMappingId(destMap == null ? -1L : destMap.getId());
        dbReplicationTbl.setDstLogicTableName("dstLogicTableName" + pk);
        dbReplicationTbl.setReplicationType(type);
        dbReplicationTbl.setDeleted(0);
        return dbReplicationTbl;
    }

    public static RowsFilterTbl buildRowsFilterTbl(long pk, String mode, String configs) {
        RowsFilterTbl rowsFilterTbl = new RowsFilterTbl();
        rowsFilterTbl.setId(pk);
        rowsFilterTbl.setMode(mode);
        rowsFilterTbl.setConfigs(configs);
        rowsFilterTbl.setDeleted(0);
        return rowsFilterTbl;
    }

    public static ColumnsFilterTbl buildColumnsFilterTbl(long pk, String mode,String columns) {
        ColumnsFilterTbl columnsFilterTbl = new ColumnsFilterTbl();
        columnsFilterTbl.setId(pk);
        columnsFilterTbl.setColumns(columns);
        columnsFilterTbl.setMode(mode);
        columnsFilterTbl.setDeleted(0);
        return columnsFilterTbl;
    }

    public static MessengerFilterTbl buildMessengerFilterTbl(long pk, String properties) {
        MessengerFilterTbl messengerFilterTbl = new MessengerFilterTbl();
        messengerFilterTbl.setId(pk);
        messengerFilterTbl.setProperties(properties);
        messengerFilterTbl.setDeleted(0);
        return messengerFilterTbl;
    }

    public static DbReplicationFilterMappingTbl buildDbReplicationFilterMappingTbl(
            Long pk,DbReplicationTbl dbReplicationTbl, Long rowsFilterId, Long columnsFilterId, Long messengerFilterId) {
        DbReplicationFilterMappingTbl dbReplicationFilterMappingTbl = new DbReplicationFilterMappingTbl();
        dbReplicationFilterMappingTbl.setId(pk);
        dbReplicationFilterMappingTbl.setDbReplicationId(dbReplicationTbl.getId());
        dbReplicationFilterMappingTbl.setRowsFilterId(rowsFilterId == null ? -1L : rowsFilterId);
        dbReplicationFilterMappingTbl.setColumnsFilterId(columnsFilterId == null ? -1L : columnsFilterId);
        dbReplicationFilterMappingTbl.setMessengerFilterId(messengerFilterId == null ? -1L : messengerFilterId);
        return dbReplicationFilterMappingTbl;
    }

    public static MigrationTaskTbl buildMigrationTaskTbl(Long pk, String oldMha, String newMha, String dbs, String operator) {
        MigrationTaskTbl migrationTaskTbl = new MigrationTaskTbl();
        migrationTaskTbl.setId(pk);
        migrationTaskTbl.setOldMha(oldMha);
        migrationTaskTbl.setNewMha(newMha);
        migrationTaskTbl.setDbs(dbs);
        migrationTaskTbl.setOperator(operator);
        migrationTaskTbl.setDeleted(0);
        return migrationTaskTbl;
    }

    public static List<ResourceView> buildResourceViews(int count,int type) {
        List<ResourceView> res = Lists.newArrayList();
        for (int i = 1; i < 1 + count; i++) {
            ResourceView resourceView = new ResourceView();
            resourceView.setResourceId(Long.valueOf(i));
            resourceView.setType(type);
            resourceView.setIp("127.0.0." + i);
            res.add(resourceView);
        }
        return res;
    }

    public static ApplierGroupTblV2 buildApplierGroupTbl(Long pk,MhaReplicationTbl mhaReplicationTbl) {
        ApplierGroupTblV2 applierGroupTbl = new ApplierGroupTblV2();
        applierGroupTbl.setId(pk);
        applierGroupTbl.setMhaReplicationId(mhaReplicationTbl.getId());
        applierGroupTbl.setDeleted(0);
        return applierGroupTbl;
    }

    public static MessengerGroupTbl buildMessengerGroupTbl(Long pk, Long mhaId) {
        MessengerGroupTbl messengerGroupTbl = new MessengerGroupTbl();
        messengerGroupTbl.setId(pk);
        messengerGroupTbl.setMhaId(mhaId);
        messengerGroupTbl.setDeleted(0);
        return messengerGroupTbl;
    }

    public static MessengerTbl buildMessengerTbl(Long pk, Long mGroupId) {
        MessengerTbl messengerTbl = new MessengerTbl();
        messengerTbl.setId(pk);
        messengerTbl.setMessengerGroupId(mGroupId);
        messengerTbl.setDeleted(0);
        return messengerTbl;
    }

}
