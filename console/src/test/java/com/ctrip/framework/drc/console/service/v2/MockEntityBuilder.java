package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.dao.entity.DbTbl;
import com.ctrip.framework.drc.console.dao.entity.MachineTbl;
import com.ctrip.framework.drc.console.dao.entity.ReplicatorGroupTbl;
import com.ctrip.framework.drc.console.dao.entity.ReplicatorTbl;
import com.ctrip.framework.drc.console.dao.entity.ResourceTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.DbReplicationTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaDbMappingTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
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


    public static ReplicatorGroupTbl buildReplicatorGroupTbl() {
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
}
