package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.dao.entity.*;
import com.ctrip.framework.drc.console.dao.entity.v2.*;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by dengquanliang
 * 2023/6/15 11:35
 */
public class MigrateEntityBuilder {

    public static List<MhaTbl> getMhaTbls() {
        List<MhaTbl> mhaTbls = new ArrayList<>();
        for (int i = 200; i <= 201; i++) {
            MhaTbl mhaTbl = new MhaTbl();
            mhaTbl.setId(Long.valueOf(i));
            mhaTbl.setMhaName("mha" + i);
            mhaTbl.setDeleted(0);
            mhaTbls.add(mhaTbl);
            mhaTbl.setDcId(1L);
            mhaTbl.setMonitorSwitch(1);
        }
        return mhaTbls;
    }

    public static List<MhaGroupTbl> getMhaGroups() {
        List<MhaGroupTbl> mhaGroups = new ArrayList<>();
        MhaGroupTbl mhaGroupTbl = new MhaGroupTbl();
        mhaGroupTbl.setId(200L);
        mhaGroupTbl.setDeleted(0);
        mhaGroups.add(mhaGroupTbl);
        mhaGroupTbl.setReadUser("readUser");
        mhaGroupTbl.setReadPassword("readPassword");
        mhaGroupTbl.setWriteUser("writerUser");
        mhaGroupTbl.setWritePassword("writePassword");
        mhaGroupTbl.setMonitorUser("monitorUser");
        mhaGroupTbl.setMonitorPassword("monitorPassword");
        return mhaGroups;
    }

    public static List<GroupMappingTbl> getGroupMappings() {
        List<GroupMappingTbl> groupMappings = new ArrayList<>();
        for (int i = 200; i <= 201; i++) {
            GroupMappingTbl groupMappingTbl = new GroupMappingTbl();
            groupMappingTbl.setDeleted(0);
            groupMappingTbl.setId(Long.valueOf(i));
            groupMappingTbl.setMhaGroupId(200L);
            groupMappingTbl.setMhaId(Long.valueOf(i));

            groupMappings.add(groupMappingTbl);
        }
        return groupMappings;
    }

    public static List<ClusterTbl> getClusterTbls() {
        List<ClusterTbl> clusterTbls = new ArrayList<>();
        ClusterTbl clusterTbl = new ClusterTbl();
        clusterTbl.setDeleted(0);
        clusterTbl.setId(200L);
        clusterTbl.setClusterName("cluster");
        clusterTbls.add(clusterTbl);
        clusterTbl.setBuId(1L);
        return clusterTbls;
    }

    public static List<ClusterMhaMapTbl> getClusterMhaMapTbl() {
        List<ClusterMhaMapTbl> clusterMhaMapTbls = new ArrayList<>();
        for (int i = 200; i <= 201; i++) {
            ClusterMhaMapTbl clusterMhaMapTbl = new ClusterMhaMapTbl();
            clusterMhaMapTbl.setDeleted(0);
            clusterMhaMapTbl.setClusterId(200L);
            clusterMhaMapTbl.setMhaId(Long.valueOf(i));
            clusterMhaMapTbls.add(clusterMhaMapTbl);
        }
        return clusterMhaMapTbls;
    }

    public static List<ReplicatorGroupTbl> getReplicatorGroupTbls() {
        List<ReplicatorGroupTbl> replicatorGroupTbls = new ArrayList<>();
        for (int i = 200; i <= 201; i++) {
            ReplicatorGroupTbl replicatorGroupTbl = new ReplicatorGroupTbl();
            replicatorGroupTbl.setDeleted(0);
            replicatorGroupTbl.setMhaId(Long.valueOf(i));
            replicatorGroupTbl.setId(Long.valueOf(i));
            replicatorGroupTbls.add(replicatorGroupTbl);
        }
        return replicatorGroupTbls;
    }

    public static ReplicatorGroupTbl getReplicatorGroupTbl() {
        return getReplicatorGroupTbls().stream().filter(e -> e.getId() == 200L).findFirst().get();
    }

    public static List<ApplierGroupTbl> getApplierGroupTbls() {
        List<ApplierGroupTbl> applierGroupTbls = new ArrayList<>();
        for (int i = 200; i <= 201; i++) {
            ApplierGroupTbl applierGroupTbl = new ApplierGroupTbl();
            applierGroupTbl.setDeleted(0);
            applierGroupTbl.setId(Long.valueOf(i));
            applierGroupTbl.setReplicatorGroupId(Long.valueOf(i));
            applierGroupTbl.setMhaId(i == 200 ? 201L : 200L);
            applierGroupTbls.add(applierGroupTbl);
            applierGroupTbl.setNameFilter("testDb\\.table" + i);
            applierGroupTbl.setNameMapping("test.db,test.db1");
            applierGroupTbl.setGtidExecuted("GTID");
            applierGroupTbl.setIncludedDbs("includedDbs");
            applierGroupTbl.setTargetName("targetName");
        }
        return applierGroupTbls;
    }

    public static ApplierGroupTbl getApplierGroupTbl() {
        return getApplierGroupTbls().stream().filter(e -> e.getId() == 200L).findFirst().get();
    }

    public static List<ApplierTbl> getApplierTbls() {
        List<ApplierTbl> applierTbls = new ArrayList<>();
        for (int i = 200; i <= 201; i++) {
            ApplierTbl applierTbl = new ApplierTbl();
            applierTbl.setApplierGroupId(Long.valueOf(i));
            applierTbl.setDeleted(0);
            applierTbl.setId(Long.valueOf(i));
            applierTbl.setMaster(1);
            applierTbl.setGtidInit("gtid");
            applierTbls.add(applierTbl);
            applierTbl.setMaster(1);
            applierTbl.setPort(80);
            applierTbl.setResourceId(200L);
        }
        return applierTbls;
    }

    public static List<ApplierTblV2> getApplierTblV2s() {
        List<ApplierTblV2> applierTbls = new ArrayList<>();
        for (int i = 200; i <= 201; i++) {
            ApplierTblV2 applierTbl = new ApplierTblV2();
            applierTbl.setApplierGroupId(200L);
            applierTbl.setDeleted(0);
            applierTbl.setId(Long.valueOf(i));
            applierTbl.setMaster(1);
            applierTbls.add(applierTbl);
            applierTbl.setMaster(1);
            applierTbl.setPort(80);
            applierTbl.setResourceId(200L);
        }
        return applierTbls;
    }

    public static List<MhaReplicationTbl> getMhaReplicationTbls() {
        MhaReplicationTbl mhaReplicationTbl = new MhaReplicationTbl();
        mhaReplicationTbl.setDeleted(0);
        mhaReplicationTbl.setId(200L);
        mhaReplicationTbl.setSrcMhaId(200L);
        mhaReplicationTbl.setDstMhaId(201L);
        return Lists.newArrayList(mhaReplicationTbl);
    }

    public static List<DataMediaTbl> getDataMediaTbls() {
        DataMediaTbl tbl1 = new DataMediaTbl();
        tbl1.setId(200L);
        tbl1.setDeleted(0);
        tbl1.setNamespcae("db200");
        tbl1.setName("table1");
        tbl1.setApplierGroupId(200L);
        tbl1.setType(0);
        tbl1.setDataMediaSourceId(-1L);

        return Lists.newArrayList(tbl1);
    }

    public static List<RowsFilterMappingTbl> getRowsFilterMappings() {
        RowsFilterMappingTbl tbl = new RowsFilterMappingTbl();
        tbl.setId(200L);
        tbl.setDeleted(0);
        tbl.setApplierGroupId(200L);
        tbl.setRowsFilterId(200L);
        tbl.setDataMediaId(200L);
        return Lists.newArrayList(tbl);
    }

    public static List<RowsFilterMappingTbl> getRowsFilterMapping() {
        List<RowsFilterMappingTbl> rowsFilterMappingTbls = new ArrayList<>();
        RowsFilterMappingTbl tbl = new RowsFilterMappingTbl();
        tbl.setId(200L);
        tbl.setApplierGroupId(200L);
        tbl.setDeleted(0);
        tbl.setDataMediaId(200L);
        tbl.setRowsFilterId(200L);
        rowsFilterMappingTbls.add(tbl);

        return rowsFilterMappingTbls;
    }

    public static List<ColumnsFilterTbl> getColumnsFilterTbls() {
        List<ColumnsFilterTbl> tbls = new ArrayList<>();
        for (int i = 200; i < 202; i++) {
            ColumnsFilterTbl tbl = new ColumnsFilterTbl();
            tbl.setDeleted(0);
            tbl.setId(Long.valueOf(i));
            tbl.setMode("exclude");
            tbls.add(tbl);
            tbl.setDataMediaId(tbl.getId());
            tbl.setColumns("udl");
        }
        return tbls;
    }

    public static ColumnsFilterTblV2 getColumnsFilterTblV2() {
        ColumnsFilterTblV2 tbl = new ColumnsFilterTblV2();
        tbl.setDeleted(0);
        tbl.setId(200L);
        tbl.setMode(0);
        tbl.setColumns("udl");
        return tbl;
    }

    public static List<RowsFilterTbl> getRowsFilterTbls() {
        List<RowsFilterTbl> tbls = new ArrayList<>();
        for (int i = 200; i < 202; i++) {
            RowsFilterTbl tbl = new RowsFilterTbl();
            tbl.setId(Long.valueOf(i));
            tbl.setMode("java_regex");
            tbl.setConfigs("configs");
            tbl.setParameters("");
            tbl.setDeleted(0);
            tbls.add(tbl);
        }
        return tbls;
    }

    public static RowsFilterTblV2 getRowsFilterTblV2() {
        RowsFilterTblV2 tbl = new RowsFilterTblV2();
        tbl.setId(200L);
        tbl.setMode(0);
        tbl.setConfigs("configs");
        tbl.setDeleted(0);
        return tbl;
    }

    public static MhaReplicationTbl getMhaReplicationTbl() {
        MhaReplicationTbl tbl = new MhaReplicationTbl();
        tbl.setDeleted(0);
        tbl.setSrcMhaId(200L);
        tbl.setDstMhaId(201L);
        tbl.setId(200L);
        return tbl;
    }

    public static List<MhaDbMappingTbl> getMhaDbMappingTbls() {
        List<MhaDbMappingTbl> tbls = new ArrayList<>();
        for (int i = 200; i <= 201; i++) {
            MhaDbMappingTbl tbl = new MhaDbMappingTbl();
            tbl.setDeleted(0);
            tbl.setMhaId(Long.valueOf(i));
            tbl.setDbId(200L);
            tbl.setId(Long.valueOf(i));
            tbls.add(tbl);
        }
        return tbls;
    }

    public static List<DbTbl> getDbTbls() {
        List<DbTbl> tbls = new ArrayList<>();
        for (int i = 200; i <= 201; i++) {
            DbTbl tbl = new DbTbl();
            tbl.setDeleted(0);
            tbl.setDbName("db" + i);
            tbl.setId(Long.valueOf(i));
            tbls.add(tbl);
        }

        return tbls;
    }

    public static MessengerGroupTbl getMessengerGroup() {
        MessengerGroupTbl tbl = new MessengerGroupTbl();
        tbl.setDeleted(0);
        tbl.setId(200L);
        tbl.setMhaId(200L);
        tbl.setReplicatorGroupId(-1L);
        tbl.setGtidExecuted("gtid");
        return tbl;
    }

    public static MessengerTbl getMessenger() {
        MessengerTbl messengerTbl = new MessengerTbl();
        messengerTbl.setId(200L);
        messengerTbl.setMessengerGroupId(200L);
        messengerTbl.setDeleted(0);
        return messengerTbl;
    }

    public static DataMediaPairTbl getDataMediaPairTbl() {
        DataMediaPairTbl tbl = new DataMediaPairTbl();
        tbl.setDeleted(0);
        tbl.setId(200L);
        tbl.setGroupId(200L);
        tbl.setSrcDataMediaName("db200\\.table1");
        tbl.setDestDataMediaName("topic");
        tbl.setProperties("properties");
        tbl.setTag("tag");
        tbl.setType(1);
        tbl.setProcessor("processor");
        return tbl;
    }

    public static List<DbReplicationTbl> getDbReplicationTbls() {
        DbReplicationTbl tbl1 = new DbReplicationTbl();
        tbl1.setDeleted(0);
        tbl1.setReplicationType(0);
        tbl1.setSrcLogicTableName("table1");
        tbl1.setSrcMhaDbMappingId(200L);
        tbl1.setDstLogicTableName("table2");
        tbl1.setDstMhaDbMappingId(201L);
        tbl1.setId(200L);

        DbReplicationTbl tbl2 = new DbReplicationTbl();
        tbl2.setDeleted(0);
        tbl2.setReplicationType(1);
        tbl2.setSrcLogicTableName("table1");
        tbl2.setSrcMhaDbMappingId(200L);
        tbl2.setDstLogicTableName("topic");
        tbl2.setDstMhaDbMappingId(-1L);
        tbl2.setId(201L);

        return Lists.newArrayList(tbl1, tbl2);
    }

    public static List<MessengerFilterTbl> getMessengerFilters() {
        MessengerFilterTbl tbl = new MessengerFilterTbl();
        tbl.setDeleted(0);
        tbl.setId(200L);
        tbl.setProperties("properties");
        return Lists.newArrayList(tbl);
    }

    public static List<ApplierGroupTblV2> getApplierGroupTblV2s() {
        ApplierGroupTblV2 applierGroupTbl = new ApplierGroupTblV2();
        applierGroupTbl.setDeleted(0);
        applierGroupTbl.setId(200L);
        applierGroupTbl.setGtidInit("applierGtId");
        applierGroupTbl.setMhaReplicationId(200L);
        return Lists.newArrayList(applierGroupTbl);
    }

    public static MhaTblV2 getMhaTblV2() {
        MhaTblV2 tbl = new MhaTblV2();
        tbl.setId(200L);
        tbl.setMhaName("mha");
        tbl.setDcId(200L);
        tbl.setBuId(200L);
        tbl.setClusterName("cluster");
        tbl.setReadUser("");
        tbl.setReadPassword("");
        tbl.setWriteUser("");
        tbl.setWritePassword("");
        tbl.setMonitorPassword("");
        tbl.setMonitorUser("");
        tbl.setDeleted(0);
        tbl.setAppId(1L);
        tbl.setApplyMode(0);

        return tbl;
    }

    public static List<MhaTblV2> getMhaTblV2s() {
        List<MhaTblV2> tbls = new ArrayList<>();
        for (int i = 200; i <= 201; i++) {
            MhaTblV2 tbl = new MhaTblV2();
            tbl.setId(Long.valueOf(i));
            tbl.setMhaName("mha" + i);
            tbl.setDcId(200L);
            tbl.setBuId(200L);
            tbl.setClusterName("cluster");
            tbl.setReadUser("");
            tbl.setReadPassword("");
            tbl.setWriteUser("");
            tbl.setWritePassword("");
            tbl.setMonitorPassword("");
            tbl.setMonitorUser("");
            tbl.setDeleted(0);
            tbl.setAppId(1L);
            tbl.setApplyMode(0);

            tbls.add(tbl);
        }

        return tbls;
    }
}
