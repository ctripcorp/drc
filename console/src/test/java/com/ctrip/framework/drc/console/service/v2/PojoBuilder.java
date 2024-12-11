package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.dao.entity.*;
import com.ctrip.framework.drc.console.dao.entity.v2.*;
import com.ctrip.framework.drc.console.dao.entity.v3.ApplierGroupTblV3;
import com.ctrip.framework.drc.console.dao.entity.v3.ApplierTblV3;
import com.ctrip.framework.drc.console.dao.entity.v3.MhaDbReplicationTbl;
import com.ctrip.framework.drc.console.dto.MhaInstanceGroupDto;
import com.ctrip.framework.drc.console.enums.ApprovalResultEnum;
import com.ctrip.framework.drc.console.enums.ReplicationTypeEnum;
import com.ctrip.framework.drc.console.enums.RowsFilterModeEnum;
import com.ctrip.framework.drc.console.enums.v2.EffectiveStatusEnum;
import com.ctrip.framework.drc.console.enums.v2.ExistingDataStatusEnum;
import com.ctrip.framework.drc.console.param.v2.RowsFilterCreateParam;
import com.ctrip.framework.drc.console.vo.v2.MhaAzView;
import com.ctrip.framework.drc.core.entity.*;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplierInfoDto;
import com.ctrip.framework.drc.core.server.config.applier.dto.FetcherInfoDto;
import com.ctrip.framework.drc.core.server.config.applier.dto.MessengerInfoDto;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.sql.Timestamp;
import java.util.*;

/**
 * Created by dengquanliang
 * 2023/6/15 11:35
 */
public class PojoBuilder {

    public static ReplicationTableTbl buildReplicationTableTbl() {
        ReplicationTableTbl tbl = new ReplicationTableTbl();
        tbl.setId(1L);
        tbl.setDbReplicationId(1L);
        tbl.setDbName("dbName");
        tbl.setTableName("tableName");
        tbl.setSrcMha("srcMha");
        tbl.setSrcRegion("srcRegion");
        tbl.setDstMha("dstMha");
        tbl.setDstRegion("dstRegion");
        tbl.setEffectiveStatus(EffectiveStatusEnum.IN_EFFECT.getCode());
        tbl.setExistingDataStatus(ExistingDataStatusEnum.NOT_PROCESSED.getCode());
        tbl.setDeleted(0);
        return tbl;
    }

    public static ApplicationRelationTbl buildApplicationRelationTbl() {
        ApplicationRelationTbl tbl = new ApplicationRelationTbl();
        tbl.setId(1L);
        tbl.setApplicationFormId(1L);
        tbl.setDbReplicationId(1L);

        return tbl;
    }

    public static ApplicationFormTbl buildApplicationFormTbl() {
        ApplicationFormTbl tbl = new ApplicationFormTbl();
        tbl.setId(1L);
        tbl.setBuName("buName");
        tbl.setDbName("db");
        tbl.setTableName("table");
        tbl.setSrcRegion("srcRegion");
        tbl.setDstRegion("dstRegion");
        tbl.setTps("tps");
        tbl.setDescription("desc");
        tbl.setDisruptionImpact("impact");
        tbl.setFilterType("ALL");
        tbl.setTag("tag");
        tbl.setFlushExistingData(1);
        tbl.setOrderRelated(1);
        tbl.setGtidInit("gtid");
        tbl.setRemark("remark");
        tbl.setCreateTime(new Timestamp(System.currentTimeMillis()));
        tbl.setDatachangeLasttime(new Timestamp(System.currentTimeMillis() - 600000L));
        tbl.setIsSentEmail(0);

        return tbl;
    }

    public static ApplicationApprovalTbl buildApplicationApprovalTbl() {
        ApplicationApprovalTbl tbl = new ApplicationApprovalTbl();
        tbl.setId(1L);
        tbl.setApplicationFormId(1L);
        tbl.setApprovalResult(ApprovalResultEnum.NOT_APPROVED.getCode());
        return tbl;
    }

    public static ApplicationApprovalTbl buildApplicationApprovalTbl1() {
        ApplicationApprovalTbl tbl = new ApplicationApprovalTbl();
        tbl.setId(1L);
        tbl.setApplicationFormId(1L);
        tbl.setApprovalResult(ApprovalResultEnum.APPROVED.getCode());
        tbl.setApplicant("applicant");
        return tbl;
    }

    public static ApplicationApprovalTbl buildApplicationApprovalTbl2() {
        ApplicationApprovalTbl tbl = new ApplicationApprovalTbl();
        tbl.setId(1L);
        tbl.setApplicationFormId(1L);
        tbl.setApprovalResult(ApprovalResultEnum.APPROVED.getCode());
        tbl.setApplicant("applicant@trip.com");
        return tbl;
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

    public static List<ReplicatorTbl> getReplicatorTbls() {
        List<ReplicatorTbl> tbls = new ArrayList<>();
        for (int i = 200; i < 202; i++) {
            ReplicatorTbl replicatorTbl = new ReplicatorTbl();
            replicatorTbl.setId(Long.valueOf(i));
            replicatorTbl.setDeleted(0);
            replicatorTbl.setRelicatorGroupId(200L);
            replicatorTbl.setResourceId(200L);
            replicatorTbl.setApplierPort(1010);
            replicatorTbl.setGtidInit("gtId");
            replicatorTbl.setPort(3030);
            replicatorTbl.setMaster(1);
            tbls.add(replicatorTbl);
        }

        return tbls;
    }


    public static List<ApplierTblV3> getApplierTblV3s() {
        List<ApplierTblV3> applierTbls = new ArrayList<>();
        for (int i = 200; i <= 201; i++) {
            ApplierTblV3 applierTbl = new ApplierTblV3();
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
        mhaReplicationTbl.setDrcStatus(1);
        return Lists.newArrayList(mhaReplicationTbl);
    }

    public static List<MhaDbReplicationTbl> getMhaDbReplicationTbls() {
        MhaDbReplicationTbl mhaReplicationTbl = new MhaDbReplicationTbl();
        mhaReplicationTbl.setDeleted(0);
        mhaReplicationTbl.setId(200L);
        mhaReplicationTbl.setSrcMhaDbMappingId(200L);
        mhaReplicationTbl.setDstMhaDbMappingId(201L);
        return Lists.newArrayList(mhaReplicationTbl);
    }

    public static List<MhaDbReplicationTbl> getMhaDbReplicationTbls2() {
        MhaDbReplicationTbl mhaReplicationTbl = new MhaDbReplicationTbl();
        mhaReplicationTbl.setDeleted(0);
        mhaReplicationTbl.setId(200L);
        mhaReplicationTbl.setSrcMhaDbMappingId(200L);
        mhaReplicationTbl.setDstMhaDbMappingId(201L);
        mhaReplicationTbl.setReplicationType(ReplicationTypeEnum.DB_TO_DB.getType());
        return Lists.newArrayList(mhaReplicationTbl);
    }

    public static List<MhaDbReplicationTbl> getMhaDbReplicationTbls01() {
        MhaDbReplicationTbl mhaReplicationTbl = new MhaDbReplicationTbl();
        mhaReplicationTbl.setDeleted(0);
        mhaReplicationTbl.setId(200L);
        mhaReplicationTbl.setSrcMhaDbMappingId(200L);
        mhaReplicationTbl.setDstMhaDbMappingId(201L);

        MhaDbReplicationTbl mhaReplicationTbl01 = new MhaDbReplicationTbl();
        mhaReplicationTbl01.setDeleted(0);
        mhaReplicationTbl01.setId(201L);
        mhaReplicationTbl01.setSrcMhaDbMappingId(201L);
        mhaReplicationTbl01.setDstMhaDbMappingId(200L);
        return Lists.newArrayList(mhaReplicationTbl, mhaReplicationTbl01);
    }

    public static List<MhaReplicationTbl> getMhaReplicationTbls1() {
        MhaReplicationTbl mhaReplicationTbl = new MhaReplicationTbl();
        mhaReplicationTbl.setDeleted(0);
        mhaReplicationTbl.setId(200L);
        mhaReplicationTbl.setSrcMhaId(200L);
        mhaReplicationTbl.setDstMhaId(201L);
        mhaReplicationTbl.setDrcStatus(1);

        MhaReplicationTbl tbl1 = new MhaReplicationTbl();
        tbl1.setDeleted(0);
        tbl1.setId(201L);
        tbl1.setSrcMhaId(201L);
        tbl1.setDstMhaId(200L);
        tbl1.setDrcStatus(1);
        return Lists.newArrayList(mhaReplicationTbl, tbl1);
    }

    public static List<MhaReplicationTbl> getMhaReplicationTbls2() {
        MhaReplicationTbl mhaReplicationTbl = new MhaReplicationTbl();
        mhaReplicationTbl.setDeleted(0);
        mhaReplicationTbl.setId(200L);
        mhaReplicationTbl.setSrcMhaId(200L);
        mhaReplicationTbl.setDstMhaId(300L);
        mhaReplicationTbl.setDrcStatus(1);

        MhaReplicationTbl tbl1 = new MhaReplicationTbl();
        tbl1.setDeleted(0);
        tbl1.setId(201L);
        tbl1.setSrcMhaId(300L);
        tbl1.setDstMhaId(200L);
        tbl1.setDrcStatus(1);
        return Lists.newArrayList(mhaReplicationTbl, tbl1);
    }


    public static ColumnsFilterTblV2 getColumnsFilterTblV2() {
        ColumnsFilterTblV2 tbl = new ColumnsFilterTblV2();
        tbl.setDeleted(0);
        tbl.setId(200L);
        tbl.setMode(0);
        tbl.setColumns("udl");
        return tbl;
    }

    public static ColumnsFilterTblV2 getColumnsFilterTbl() {
        ColumnsFilterTblV2 tbl = new ColumnsFilterTblV2();
        tbl.setDeleted(0);
        tbl.setId(200L);
        tbl.setMode(0);
        tbl.setColumns(JsonUtils.toJson(Lists.newArrayList("column")));
        return tbl;
    }

    public static RowsFilterTblV2 getRowsFilterTbl() {
        RowsFilterCreateParam rowsFilterCreateParam = getRowsFilterCreateParam();
        RowsFilterTblV2 rowsFilterTblV2 = rowsFilterCreateParam.extractRowsFilterTbl();
        rowsFilterTblV2.setId(200L);
        return rowsFilterTblV2;
    }

    public static RowsFilterCreateParam getRowsFilterCreateParam() {
        RowsFilterCreateParam param = new RowsFilterCreateParam();
        param.setDbReplicationIds(Lists.newArrayList(200L, 201L));
        param.setMode(RowsFilterModeEnum.TRIP_UDL.getCode());
//        param.setColumns(Lists.newArrayList("uid"));
        param.setUdlColumns(Lists.newArrayList("udl"));
        param.setDrcStrategyId(1);
        param.setRouteStrategyId(0);
        param.setContext("context");
        param.setIllegalArgument(false);
        param.setFetchMode(0);

        return param;
    }

    public static MhaReplicationTbl getMhaReplicationTbl() {
        MhaReplicationTbl tbl = new MhaReplicationTbl();
        tbl.setDeleted(0);
        tbl.setSrcMhaId(200L);
        tbl.setDstMhaId(201L);
        tbl.setId(200L);
        return tbl;
    }

    public static List<MhaDbMappingTbl> getMhaDbMappingTbls1() {
        List<MhaDbMappingTbl> tbls = new ArrayList<>();
        for (int i = 200; i <= 201; i++) {
            MhaDbMappingTbl tbl = new MhaDbMappingTbl();
            tbl.setDeleted(0);
            tbl.setMhaId(Long.valueOf(i));
            tbl.setDbId(Long.valueOf(i));
            tbl.setId(Long.valueOf(i));
            tbls.add(tbl);
        }
        return tbls;
    }

    public static List<MhaDbMappingTbl> getMhaDbMappingTbls2() {
        List<MhaDbMappingTbl> tbls = new ArrayList<>();
        for (int i = 200; i <= 202; i++) {
            MhaDbMappingTbl tbl = new MhaDbMappingTbl();
            tbl.setDeleted(0);
            tbl.setMhaId(Long.valueOf(i));
            tbl.setDbId(200L);
            tbl.setId(Long.valueOf(i));
            tbls.add(tbl);
        }
        return tbls;
    }

    public static List<MhaDbMappingTbl> getMhaDbMappingTbls3() {
        List<MhaDbMappingTbl> tbls = new ArrayList<>();
        for (int i = 200; i <= 202; i++) {
            MhaDbMappingTbl tbl = new MhaDbMappingTbl();
            tbl.setDeleted(0);
            tbl.setMhaId(Long.valueOf(i));
            tbl.setDbId(200L);
            tbl.setId(Long.valueOf(i + 10));
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
            tbl.setDbOwner("dbOwner" + i);
            tbl.setEmailGroup("test@trip.com");
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

    public static List<MessengerGroupTbl> getMessengerGroups() {
        List<MessengerGroupTbl> tbls = new ArrayList<>();
        for (int i = 200; i <= 201; i++) {
            MessengerGroupTbl tbl = new MessengerGroupTbl();
            tbl.setDeleted(0);
            tbl.setId(Long.valueOf(i));
            tbl.setMhaId(200L);
            tbl.setReplicatorGroupId(-1L);
            tbl.setGtidExecuted("gtid");
            tbls.add(tbl);
        }
        return tbls;
    }

    public static MessengerTbl getMessenger() {
        MessengerTbl messengerTbl = new MessengerTbl();
        messengerTbl.setId(200L);
        messengerTbl.setMessengerGroupId(200L);
        messengerTbl.setDeleted(0);
        return messengerTbl;
    }

    public static List<MessengerTbl> getMessengers() {
        MessengerTbl messengerTbl = new MessengerTbl();
        messengerTbl.setId(200L);
        messengerTbl.setMessengerGroupId(200L);
        messengerTbl.setDeleted(0);
        messengerTbl.setResourceId(200L);

        MessengerTbl messengerTbl02 = new MessengerTbl();
        messengerTbl02.setId(201L);
        messengerTbl02.setMessengerGroupId(200L);
        messengerTbl02.setDeleted(0);
        messengerTbl02.setResourceId(201L);
        return Lists.newArrayList(messengerTbl, messengerTbl02);
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
        tbl1.setCreateTime(new Timestamp(System.currentTimeMillis()));
        tbl1.setDatachangeLasttime(new Timestamp(System.currentTimeMillis()));

        DbReplicationTbl tbl2 = new DbReplicationTbl();
        tbl2.setDeleted(0);
        tbl2.setReplicationType(1);
        tbl2.setSrcLogicTableName("table1");
        tbl2.setSrcMhaDbMappingId(200L);
        tbl2.setDstLogicTableName("topic");
        tbl2.setDstMhaDbMappingId(-1L);
        tbl2.setId(201L);

        tbl2.setCreateTime(new Timestamp(System.currentTimeMillis()));
        tbl2.setDatachangeLasttime(new Timestamp(System.currentTimeMillis()));
        return Lists.newArrayList(tbl1, tbl2);
    }

    public static List<DbReplicationTbl> getDbReplicationTbls1() {
        DbReplicationTbl tbl1 = new DbReplicationTbl();
        tbl1.setDeleted(0);
        tbl1.setReplicationType(0);
        tbl1.setSrcLogicTableName("table1");
        tbl1.setSrcMhaDbMappingId(200L);
        tbl1.setDstLogicTableName("table2");
        tbl1.setDstMhaDbMappingId(201L);
        tbl1.setId(200L);
        tbl1.setCreateTime(new Timestamp(System.currentTimeMillis()));
        tbl1.setDatachangeLasttime(new Timestamp(System.currentTimeMillis()));

        return Lists.newArrayList(tbl1);
    }

    public static List<ApplierGroupTblV3> getApplierGroupTblV3s() {
        ApplierGroupTblV3 applierGroupTbl = new ApplierGroupTblV3();
        applierGroupTbl.setDeleted(0);
        applierGroupTbl.setId(200L);
        applierGroupTbl.setGtidInit("applierGtId");
        applierGroupTbl.setMhaDbReplicationId(200L);
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
        tbl.setTag("tag");

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

    public static List<DcTbl> getDcTbls() {
        List<DcTbl> dcTbls = new ArrayList<>();
        DcTbl tbl1 = new DcTbl();
        tbl1.setId(200L);
        tbl1.setDcName("shaxy");
        tbl1.setRegionName("sha");

        DcTbl tb2 = new DcTbl();
        tb2.setId(201L);
        tb2.setDcName("sinaws");
        tb2.setRegionName("sin");

        dcTbls.add(tbl1);
        dcTbls.add(tb2);
        return dcTbls;
    }

    public static BuTbl getBuTbl() {
        BuTbl buTbl = new BuTbl();
        buTbl.setBuName("BBZ");
        buTbl.setId(200L);
        return buTbl;
    }

    public static List<ResourceTbl> getResourceTbls() {
        ResourceTbl resourceTbl = new ResourceTbl();
        resourceTbl.setId(200L);
        resourceTbl.setType(0);
        resourceTbl.setAz("AZ");
        resourceTbl.setIp("ip");
        resourceTbl.setTag("tag");
        resourceTbl.setDeleted(1);
        resourceTbl.setDcId(200L);
        return Lists.newArrayList(resourceTbl);
    }

    public static List<ResourceTbl> getReplicatorResources() {
        ResourceTbl resourceTbl = new ResourceTbl();
        resourceTbl.setId(200L);
        resourceTbl.setType(0);
        resourceTbl.setAz("AZ");
        resourceTbl.setIp("ip1");
        resourceTbl.setTag("tag");
        resourceTbl.setActive(1);

        ResourceTbl resourceTbl1 = new ResourceTbl();
        resourceTbl1.setId(301L);
        resourceTbl1.setType(0);
        resourceTbl1.setAz("AZ");
        resourceTbl1.setIp("ip2");
        resourceTbl1.setTag("tag");
        resourceTbl1.setActive(1);
        return Lists.newArrayList(resourceTbl, resourceTbl1);
    }

    public static List<ResourceTbl> getApplierResources() {
        ResourceTbl resourceTbl = new ResourceTbl();
        resourceTbl.setId(200L);
        resourceTbl.setType(1);
        resourceTbl.setAz("AZ");
        resourceTbl.setIp("ip1");
        resourceTbl.setTag("tag");
        resourceTbl.setActive(1);

        ResourceTbl resourceTbl1 = new ResourceTbl();
        resourceTbl1.setId(301L);
        resourceTbl1.setType(1);
        resourceTbl1.setAz("AZ");
        resourceTbl1.setIp("ip2");
        resourceTbl1.setTag("tag");
        resourceTbl1.setActive(1);
        return Lists.newArrayList(resourceTbl, resourceTbl1);
    }


    public static List<DbReplicationFilterMappingTbl> getFilterMappings() {
        DbReplicationFilterMappingTbl tbl = new DbReplicationFilterMappingTbl();
        tbl.setId(200L);
        tbl.setDbReplicationId(200L);
        tbl.setColumnsFilterId(200L);
        tbl.setRowsFilterId(200L);
        tbl.setMessengerFilterId(200L);

        return Lists.newArrayList(tbl);
    }

    public static Map<String, MhaInstanceGroupDto> getMhaInstanceGroups() {
        MhaInstanceGroupDto dto1 = new MhaInstanceGroupDto();
        dto1.setMhaName("mha200");
        MhaInstanceGroupDto.MySQLInstance master1 = new MhaInstanceGroupDto.MySQLInstance();
        master1.setIdc("AZ");
        master1.setIp("ip");
        master1.setPort(0);
        dto1.setMaster(master1);
        MhaInstanceGroupDto.MySQLInstance slave1 = new MhaInstanceGroupDto.MySQLInstance();
        slave1.setIdc("AZ");
        slave1.setIp("ip");
        slave1.setPort(0);
        dto1.setSlaves(new ArrayList<>(Arrays.asList(slave1)));

        Map<String, MhaInstanceGroupDto> result = new HashMap<>();
        result.put(dto1.getMhaName(), dto1);
        return result;
    }

    public static Drc getDrc() {
        Applier applier = new Applier();
        applier.setIp("ip");
        applier.setPort(0);
        Replicator replicator = new Replicator();
        replicator.setIp("ip");
        replicator.setPort(0);
        DbCluster dbCluster = new DbCluster();
        dbCluster.addApplier(applier);
        dbCluster.addReplicator(replicator);
        dbCluster.setId("dbClusterId");
        Dc dc = new Dc();
        dc.addDbCluster(dbCluster);
        dc.setId("dcId");
        Drc drc = new Drc();
        drc.addDc(dc);
        return drc;
    }

    public static Drc getDrc2() {
        Applier applier = new Applier();
        applier.setIp("ip");
        applier.setPort(0);
        Messenger messenger = new Messenger();
        messenger.setIp("ip");
        messenger.setPort(0);
        Replicator replicator = new Replicator();
        replicator.setIp("ip");
        replicator.setPort(0);
        replicator.setMaster(true);
        DbCluster dbCluster = new DbCluster();
        dbCluster.addApplier(applier);
        dbCluster.addMessenger(messenger);
        dbCluster.addReplicator(replicator);
        dbCluster.setId("dbClusterId");
        Dc dc = new Dc();
        dc.addDbCluster(dbCluster);
        dc.setId("dcId");
        dc.setRegion("sha");
        Drc drc = new Drc();
        drc.addDc(dc);
        return drc;
    }

    public static MhaAzView getMhaAzView() {
        MhaAzView view = new MhaAzView();
        Map<String, Set<String>> az2mhaName = new HashMap<>();
        Map<String, List<String>> az2DbInstance = new HashMap<>();
        Map<String, List<String>> az2ReplicatorInstance = new HashMap<>();
        Map<String, List<? extends FetcherInfoDto>> az2ApplierInstance = new HashMap<>();
        Map<String, List<? extends FetcherInfoDto>> az2MessengerInstance = new HashMap<>();
        Map<String, Set<String>> az2DrcDbs = new HashMap<>();

        az2mhaName.put("AZ", Sets.newHashSet("testMha"));
        az2DbInstance.put("AZ", Lists.newArrayList("ip:port"));
        az2ReplicatorInstance.put("AZ", Lists.newArrayList("ip:port"));
        az2ApplierInstance.put("AZ", Lists.newArrayList(new ApplierInfoDto()));
        az2MessengerInstance.put("AZ", Lists.newArrayList(new MessengerInfoDto()));
        az2DrcDbs.put("AZ", Sets.newHashSet("testDb"));

        view.setAz2mhaName(az2mhaName);
        view.setAz2DbInstance(az2DbInstance);
        view.setAz2ApplierInstance(az2ApplierInstance);
        view.setAz2ReplicatorInstance(az2ReplicatorInstance);
        view.setAz2MessengerInstance(az2MessengerInstance);
        view.setAz2DrcDb(az2DrcDbs);
        return view;
    }

    public static List<ApplierInfoDto> getApplierInfoDtos() {
        ApplierInfoDto applier1 = new ApplierInfoDto();
        applier1.setIp("ip");
        applier1.setMaster(true);
        ApplierInfoDto applier2 = new ApplierInfoDto();
        applier2.setIp("ip");
        applier2.setMaster(false);

        List<ApplierInfoDto> dtos = Lists.newArrayList();
        dtos.add(applier1);
        dtos.add(applier2);
        return dtos;
    }
}
