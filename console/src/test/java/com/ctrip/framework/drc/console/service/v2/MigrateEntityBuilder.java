package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.dao.entity.*;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaReplicationTbl;
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
        for (int i = 100; i < 110; i++) {
            MhaTbl mhaTbl = new MhaTbl();
            mhaTbl.setId(Long.valueOf(i));
            mhaTbl.setMhaName("mha" + i);
            mhaTbl.setDeleted(0);
            mhaTbls.add(mhaTbl);
        }
        return mhaTbls;
    }

    public static List<MhaGroupTbl> getMhaGroups() {
        List<MhaGroupTbl> mhaGroups = new ArrayList<>();
        for (int i = 100; i < 105; i++) {
            MhaGroupTbl mhaGroupTbl = new MhaGroupTbl();
            mhaGroupTbl.setId(Long.valueOf(i));
            mhaGroupTbl.setDeleted(0);
            mhaGroups.add(mhaGroupTbl);
        }
        return mhaGroups;
    }

    public static List<GroupMappingTbl> getGroupMappings() {
        List<GroupMappingTbl> groupMappings = new ArrayList<>();
        for (int i = 100; i < 110; i++) {
            GroupMappingTbl groupMappingTbl = new GroupMappingTbl();
            groupMappingTbl.setDeleted(0);
            groupMappingTbl.setId(Long.valueOf(i));
            int mhaGroupId = i < 105 ? i : 209 - i;
            groupMappingTbl.setMhaGroupId(Long.valueOf(mhaGroupId));
            groupMappingTbl.setMhaId(Long.valueOf(i));

            groupMappings.add(groupMappingTbl);
        }
        return groupMappings;
    }

    public static List<ClusterTbl> getClusterTbls() {
        List<ClusterTbl> clusterTbls = new ArrayList<>();
        for (int i = 100; i < 110; i++) {
            ClusterTbl clusterTbl = new ClusterTbl();
            clusterTbl.setDeleted(0);
            clusterTbl.setId(Long.valueOf(i));
            clusterTbl.setClusterName("cluster" + i);
            clusterTbls.add(clusterTbl);
        }
        return clusterTbls;
    }

    public static List<ClusterMhaMapTbl> getClusterMhaMapTbl() {
        List<ClusterMhaMapTbl> clusterMhaMapTbls = new ArrayList<>();
        for (int i = 100; i < 110; i++) {
            ClusterMhaMapTbl clusterMhaMapTbl = new ClusterMhaMapTbl();
            clusterMhaMapTbl.setDeleted(0);
            clusterMhaMapTbl.setClusterId(Long.valueOf(i));
            clusterMhaMapTbl.setMhaId(Long.valueOf(i));
            clusterMhaMapTbls.add(clusterMhaMapTbl);
        }
        return clusterMhaMapTbls;
    }

    public static List<ReplicatorGroupTbl> getReplicatorGroupTbls() {
        List<ReplicatorGroupTbl> replicatorGroupTbls = new ArrayList<>();
        for (int i = 100; i < 110; i++) {
            ReplicatorGroupTbl replicatorGroupTbl = new ReplicatorGroupTbl();
            replicatorGroupTbl.setDeleted(0);
            replicatorGroupTbl.setMhaId(Long.valueOf(i));
            replicatorGroupTbl.setId(Long.valueOf(i));
            replicatorGroupTbls.add(replicatorGroupTbl);
        }
        return replicatorGroupTbls;
    }

    public static List<ApplierGroupTbl> getApplierGroupTbls() {
        List<ApplierGroupTbl> applierGroupTbls = new ArrayList<>();
        for (int i = 100; i < 110; i++) {
            ApplierGroupTbl applierGroupTbl = new ApplierGroupTbl();
            applierGroupTbl.setDeleted(0);
            applierGroupTbl.setId(Long.valueOf(i));
            applierGroupTbl.setReplicatorGroupId(Long.valueOf(i));
            applierGroupTbl.setMhaId(Long.valueOf(209 - i));
            applierGroupTbls.add(applierGroupTbl);
            applierGroupTbl.setNameFilter("testDb\\.table" + i);
            applierGroupTbl.setNameMapping("test.db,test.db1");
        }
        return applierGroupTbls;
    }

    public static List<ApplierTbl> getApplierTbls() {
        List<ApplierTbl> applierTbls = new ArrayList<>();
        for (int i = 100; i < 110; i++) {
            ApplierTbl applierTbl = new ApplierTbl();
            applierTbl.setApplierGroupId(Long.valueOf(i));
            applierTbl.setDeleted(0);
            applierTbl.setId(Long.valueOf(i));
            applierTbls.add(applierTbl);
        }
        return applierTbls;
    }

    public static List<MhaReplicationTbl> getMhaReplicationTbls() {
        MhaReplicationTbl mhaReplicationTbl = new MhaReplicationTbl();
        mhaReplicationTbl.setDeleted(0);
        mhaReplicationTbl.setId(100L);
        mhaReplicationTbl.setSrcMhaId(100L);
        mhaReplicationTbl.setDstMhaId(109L);
        return Lists.newArrayList(mhaReplicationTbl);
    }

    public static List<DataMediaTbl> getDataMediaTbls() {
        List<DataMediaTbl> dataMediaTbls = new ArrayList<>();
        for (int i = 100; i < 104; i++) {
            DataMediaTbl dataMediaTbl = new DataMediaTbl();
            dataMediaTbl.setId(Long.valueOf(i));
            dataMediaTbl.setDeleted(0);
            dataMediaTbl.setNamespcae("testDb");
            String name = i % 2 == 0 ? "table" : "table" + i;
            dataMediaTbl.setName(name);
            dataMediaTbl.setApplierGroupId(Long.valueOf(i));
            dataMediaTbls.add(dataMediaTbl);
        }

        for (int i = 106; i < 110; i++) {
            DataMediaTbl dataMediaTbl = new DataMediaTbl();
            dataMediaTbl.setId(Long.valueOf(i));
            dataMediaTbl.setDeleted(0);
            dataMediaTbl.setNamespcae("testDb");
            String name = i % 2 == 0 ? "table" : "table" + i;
            dataMediaTbl.setName(name);
            dataMediaTbls.add(dataMediaTbl);
        }

        return dataMediaTbls;
    }

    public static List<RowsFilterMappingTbl> getRowsFilterMapping() {
        List<RowsFilterMappingTbl> rowsFilterMappingTbls = new ArrayList<>();
        for (int i = 106; i < 110; i++) {
            RowsFilterMappingTbl tbl = new RowsFilterMappingTbl();
            tbl.setApplierGroupId(Long.valueOf(i));
            tbl.setDeleted(0);
            tbl.setDataMediaId(Long.valueOf(i));
            rowsFilterMappingTbls.add(tbl);
        }

        return rowsFilterMappingTbls;
    }
}
