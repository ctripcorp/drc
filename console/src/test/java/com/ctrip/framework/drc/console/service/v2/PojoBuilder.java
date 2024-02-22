package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.dao.entity.*;
import com.ctrip.framework.drc.console.dao.entity.v2.*;
import com.ctrip.framework.drc.console.dao.entity.v3.ApplierGroupTblV3;
import com.ctrip.framework.drc.console.dao.entity.v3.ApplierTblV3;
import com.ctrip.framework.drc.console.dao.entity.v3.MhaDbReplicationTbl;
import com.ctrip.framework.drc.console.enums.RowsFilterModeEnum;
import com.ctrip.framework.drc.console.param.v2.RowsFilterCreateParam;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.google.common.collect.Lists;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by dengquanliang
 * 2023/6/15 11:35
 */
public class PojoBuilder {

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

    public static MessengerTbl getMessenger() {
        MessengerTbl messengerTbl = new MessengerTbl();
        messengerTbl.setId(200L);
        messengerTbl.setMessengerGroupId(200L);
        messengerTbl.setDeleted(0);
        return messengerTbl;
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

    public static List<ApplierGroupTblV2> getApplierGroupTblV2s() {
        ApplierGroupTblV2 applierGroupTbl = new ApplierGroupTblV2();
        applierGroupTbl.setDeleted(0);
        applierGroupTbl.setId(200L);
        applierGroupTbl.setGtidInit("applierGtId");
        applierGroupTbl.setMhaReplicationId(200L);
        return Lists.newArrayList(applierGroupTbl);
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
        return Lists.newArrayList(resourceTbl);
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
}
