package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.dao.entity.*;
import com.ctrip.framework.drc.console.dao.entity.v2.*;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.core.monitor.enums.ModuleEnum;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by dengquanliang
 * 2023/6/1 15:38
 */
public class MetaGeneratorBuilder {

    public static List<DcTbl> getDcTbls() {
        DcTbl dcTbl = new DcTbl();
        dcTbl.setId(100L);
        dcTbl.setDeleted(BooleanEnum.FALSE.getCode());
        dcTbl.setDcName("dc");
        dcTbl.setRegionName("region");
        return Lists.newArrayList(dcTbl);
    }

    public static List<RouteTbl> getRouteTbls() {
        RouteTbl routeTbl = new RouteTbl();
        routeTbl.setDeleted(0);
        routeTbl.setSrcDcId(100L);
        routeTbl.setDstDcId(100L);
        routeTbl.setId(100L);
        routeTbl.setRouteOrgId(100L);
        routeTbl.setSrcProxyIds("100");
        routeTbl.setOptionalProxyIds("101");
        routeTbl.setDstProxyIds("102");
        routeTbl.setTag("console");

        return Lists.newArrayList(routeTbl);
    }

    public static List<ProxyTbl> getProxyTbls() {
        List<ProxyTbl> proxyTbls = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            ProxyTbl proxyTbl = new ProxyTbl();
            proxyTbl.setDeleted(0);
            proxyTbl.setId(Long.valueOf(i + 100));
            proxyTbl.setUri("uri" + i);
            proxyTbls.add(proxyTbl);
            proxyTbl.setDcId(100L);
            proxyTbl.setActive(0);
            proxyTbl.setMonitorActive(0);
        }
        return proxyTbls;
    }

    public static List<ResourceTbl> getResourceTbls() {
        List<ResourceTbl> resourceTbls = new ArrayList<>();
        for (ModuleEnum value : ModuleEnum.values()) {
            ResourceTbl resourceTbl = new ResourceTbl();
            resourceTbl.setDeleted(0);
            resourceTbl.setId(Long.valueOf(value.getCode() + 100));
            resourceTbl.setIp("127.0.2." + value.getCode());
            resourceTbls.add(resourceTbl);
            resourceTbl.setType(value.getCode());
            resourceTbl.setDcId(100L);
            resourceTbl.setAppId(100023928L);
        }

        ResourceTbl resourceTbl = new ResourceTbl();
        resourceTbl.setDeleted(0);
        resourceTbl.setId(110L);
        resourceTbl.setIp("127.0.1.1");
        resourceTbls.add(resourceTbl);
        resourceTbl.setType(-1);
        resourceTbl.setDcId(100L);
        resourceTbl.setAppId(100023928L);
        return resourceTbls;
    }

    public static List<ClusterManagerTbl> getClusterManagerTbls() {
        ClusterManagerTbl clusterManagerTbl = new ClusterManagerTbl();
        clusterManagerTbl.setDeleted(0);
        clusterManagerTbl.setPort(80);
        clusterManagerTbl.setMaster(1);
        clusterManagerTbl.setResourceId(Long.valueOf(ModuleEnum.CLUSTER_MANAGER.getCode() + 100));
        return Lists.newArrayList(clusterManagerTbl);
    }

    public static List<ZookeeperTbl> getZookeeperTbls() {
        ZookeeperTbl zookeeperTbl = new ZookeeperTbl();
        zookeeperTbl.setDeleted(0);
        zookeeperTbl.setPort(80);
        zookeeperTbl.setResourceId(Long.valueOf(ModuleEnum.ZOOKEEPER.getCode() + 100));
        return Lists.newArrayList(zookeeperTbl);
    }

    public static List<MhaTblV2> getMhaTbls() {
        MhaTblV2 mhaTbl = new MhaTblV2();
        mhaTbl.setDeleted(0);
        mhaTbl.setMhaName("mhaA");
        mhaTbl.setId(100L);
        mhaTbl.setDcId(100L);
        mhaTbl.setBuId(100L);
        mhaTbl.setClusterName("cluster");
        mhaTbl.setReadUser("readUser");
        mhaTbl.setReadPassword("readPassword");
        mhaTbl.setWriteUser("writeUser");
        mhaTbl.setWritePassword("writePassword");
        mhaTbl.setMonitorUser("monitorUser");
        mhaTbl.setMonitorPassword("monitorPassword");
        mhaTbl.setApplyMode(0);
        mhaTbl.setAppId(100023298L);
        mhaTbl.setMonitorSwitch(0);

        return Lists.newArrayList(mhaTbl);
    }

    public static List<BuTbl> getButbls() {
        BuTbl buTbl = new BuTbl();
        buTbl.setDeleted(0);
        buTbl.setBuName("BU");
        buTbl.setId(100L);
        return Lists.newArrayList(buTbl);
    }

    public static List<MachineTbl> getMachineTbls() {
        MachineTbl machineTbl = new MachineTbl();
        machineTbl.setDeleted(0);
        machineTbl.setMhaId(100L);
        machineTbl.setMaster(1);
        machineTbl.setUuid("uuid");
        machineTbl.setIp("127.0.0.1");
        machineTbl.setPort(3306);
        return Lists.newArrayList(machineTbl);
    }

    public static List<ReplicatorGroupTbl> getReplicatorGroupTbls() {
        ReplicatorGroupTbl replicatorGroupTbl = new ReplicatorGroupTbl();
        replicatorGroupTbl.setDeleted(0);
        replicatorGroupTbl.setId(100L);
        replicatorGroupTbl.setMhaId(100L);
        replicatorGroupTbl.setExcludedTables("");
        return Lists.newArrayList(replicatorGroupTbl);
    }

    public static List<ReplicatorTbl> getReplicatorTbls() {
        ReplicatorTbl replicatorTbl = new ReplicatorTbl();
        replicatorTbl.setDeleted(0);
        replicatorTbl.setRelicatorGroupId(100L);
        replicatorTbl.setResourceId(Long.valueOf(ModuleEnum.REPLICATOR.getCode()) + 100);
        replicatorTbl.setApplierPort(1010);
        replicatorTbl.setGtidInit("gtId");
        replicatorTbl.setPort(3030);
        replicatorTbl.setMaster(1);
        return Lists.newArrayList(replicatorTbl);
    }

    public static List<MhaReplicationTbl> getMhaReplicationTbls() {
        MhaReplicationTbl mhaReplicationTbl = new MhaReplicationTbl();
        mhaReplicationTbl.setDeleted(0);
        mhaReplicationTbl.setId(100L);
        mhaReplicationTbl.setSrcMhaId(100L);
        mhaReplicationTbl.setDstMhaId(100L);
        return Lists.newArrayList(mhaReplicationTbl);
    }

    public static List<ApplierGroupTblV2> getApplierGroupTbls() {
        ApplierGroupTblV2 applierGroupTbl = new ApplierGroupTblV2();
        applierGroupTbl.setDeleted(0);
        applierGroupTbl.setId(200L);
        applierGroupTbl.setGtidInit("applierGtId");
        applierGroupTbl.setMhaReplicationId(200L);
        return Lists.newArrayList(applierGroupTbl);
    }

    public static List<MhaDbMappingTbl> getMhaDbMappingTbls() {
        MhaDbMappingTbl mhaDbMappingTbl = new MhaDbMappingTbl();
        mhaDbMappingTbl.setDeleted(0);
        mhaDbMappingTbl.setMhaId(100L);
        mhaDbMappingTbl.setDbId(100L);
        mhaDbMappingTbl.setId(100L);

        return Lists.newArrayList(mhaDbMappingTbl);
    }

    public static List<DbTbl> getDbTbls() {
        DbTbl dbTbl = new DbTbl();
        dbTbl.setDeleted(0);
        dbTbl.setId(100L);
        dbTbl.setDbName("db");
        dbTbl.setDbOwner("");
        dbTbl.setBuCode("buCode");
        dbTbl.setBuName("BU");
        dbTbl.setIsDrc(0);
        return Lists.newArrayList(dbTbl);
    }

    public static List<DbReplicationTbl> getDbReplicationTbls() {
        DbReplicationTbl tbl = new DbReplicationTbl();
        tbl.setDeleted(0);
        tbl.setSrcMhaDbMappingId(100L);
        tbl.setDstMhaDbMappingId(100L);
        tbl.setSrcLogicTableName("srcTable");
        tbl.setDstLogicTableName("dstTable");
        tbl.setId(100L);
        tbl.setReplicationType(1);

//        DbReplicationTbl tbl1 = new DbReplicationTbl();
//        tbl.setDeleted(0);
//        tbl.setSrcMhaDbMappingId(100L);
//        tbl.setDstMhaDbMappingId(-100L);
//        tbl.setSrcLogicTableName("srcTable");
//        tbl.setDstLogicTableName("topic");
//        tbl.setId(2L);
//        tbl.setReplicationType(1);
        return Lists.newArrayList(tbl);
    }

    public static List<ApplierTblV2> getApplierTbls() {
        ApplierTblV2 tbl = new ApplierTblV2();
        tbl.setId(100L);
        tbl.setDeleted(0);
        tbl.setResourceId(Long.valueOf(ModuleEnum.APPLIER.getCode() + 100));
        tbl.setMaster(1);
        tbl.setPort(2020);
        tbl.setApplierGroupId(100L);
        return Lists.newArrayList(tbl);
    }

    public static List<MessengerGroupTbl> getMessengerGroupTbls() {
        MessengerGroupTbl tbl = new MessengerGroupTbl();
        tbl.setDeleted(0);
        tbl.setId(100L);
        tbl.setGtidExecuted("messengerGtId");
        tbl.setMhaId(100L);
        tbl.setReplicatorGroupId(100L);

        return Lists.newArrayList(tbl);
    }

    public static List<MessengerTbl> getMessengerTbls() {
        MessengerTbl tbl = new MessengerTbl();
        tbl.setDeleted(0);
        tbl.setId(100L);
        tbl.setResourceId(110L);
        tbl.setPort(30);
        tbl.setMessengerGroupId(100L);

        return Lists.newArrayList(tbl);
    }

    public static List<DbReplicationFilterMappingTbl> getFilterMappingTbls() {
        DbReplicationFilterMappingTbl tbl = new DbReplicationFilterMappingTbl();
        tbl.setDeleted(0);
        tbl.setDbReplicationId(100L);
        tbl.setColumnsFilterId(100L);
        tbl.setRowsFilterId(100L);
        tbl.setMessengerFilterId(100L);
        tbl.setId(100L);

        return Lists.newArrayList(tbl);
    }

    public static List<RowsFilterTbl> getRowsFilterTbls() {
        RowsFilterTbl tbl = new RowsFilterTbl();
        tbl.setDeleted(0);
        tbl.setId(100L);
        tbl.setMode("mode");
        tbl.setConfigs("{\"parameterList\":[{\"columns\":[\"AgentUID\"],\"illegalArgument\":false,\"context\":\"SIN\",\"fetchMode\":0,\"userFilterMode\":\"uid\"}],\"drcStrategyId\":0,\"routeStrategyId\":0}");
        tbl.setParameters("");

        return Lists.newArrayList(tbl);
    }

    public static List<ColumnsFilterTblV2> getColumnsFilterTbls() {
        ColumnsFilterTblV2 tbl = new ColumnsFilterTblV2();
        tbl.setDeleted(0);
        tbl.setId(100L);
        tbl.setMode(0);
        tbl.setColumns("[\"column\"]");

        return Lists.newArrayList(tbl);
    }

    public static List<MessengerFilterTbl> getMessengerFilterTbls() {
        MessengerFilterTbl tbl = new MessengerFilterTbl();
        tbl.setDeleted(0);
        tbl.setProperties("{\"mqType\":\"qmq\",\"serialization\":\"json\",\"persistent\":false,\"order\":true,\"orderKey\":\"id\",\"delayTime\":0}");
        tbl.setId(100L);
        return Lists.newArrayList(tbl);
    }
}
