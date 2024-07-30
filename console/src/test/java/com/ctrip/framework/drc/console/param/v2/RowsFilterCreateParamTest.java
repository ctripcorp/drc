package com.ctrip.framework.drc.console.param.v2;

import com.ctrip.framework.drc.console.dao.entity.v2.RowsFilterTblV2;
import com.ctrip.framework.drc.console.enums.RowsFilterModeEnum;
import com.ctrip.framework.drc.core.server.common.filter.row.FetchMode;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

public class RowsFilterCreateParamTest {

    String udlConfigs = "{\"parameterList\":[{\"columns\":[\"userdata_location\"],\"illegalArgument\":false,\"context\":\"SIN\",\"fetchMode\":0,\"userFilterMode\":\"udl\"}],\"drcStrategyId\":2000000002,\"routeStrategyId\":0}";
    String uidConfigs = "{\"parameterList\":[{\"columns\":[\"uid\"],\"illegalArgument\":false,\"context\":\"SIN\",\"fetchMode\":0,\"userFilterMode\":\"uid\"}],\"drcStrategyId\":0,\"routeStrategyId\":0}";

    String udlThenUidConfigs = "{\"parameterList\":[{\"columns\":[\"userdata_location\"],\"illegalArgument\":false,\"context\":\"SIN\",\"fetchMode\":0,\"userFilterMode\":\"udl\"},{\"columns\":[\"uid\"],\"illegalArgument\":false,\"context\":\"SIN\",\"fetchMode\":0,\"userFilterMode\":\"uid\"}],\"drcStrategyId\":2000000002,\"routeStrategyId\":0}";
    String customSoaConfigs = "{\"parameterList\":[{\"columns\":[\"uid\"],\"illegalArgument\":false,\"context\":\"{\\\"code\\\":32578,\\\"name\\\":\\\"DataSyncService\\\"}\",\"fetchMode\":0,\"userFilterMode\":\"uid\"}],\"drcStrategyId\":0,\"routeStrategyId\":0}";
    @Test
    public void testCheckAndGetConfigs() throws Exception {
        RowsFilterCreateParam udlRowsFilterParam = getUdlRowsFilterParam();
        udlRowsFilterParam.checkParam();
        RowsFilterTblV2 udlTbl = udlRowsFilterParam.extractRowsFilterTbl();
        Assert.assertEquals(udlConfigs, udlTbl.getConfigs());


        RowsFilterCreateParam uidRowsFilterParam = getUidRowsFilterParam();
        uidRowsFilterParam.checkParam();
        RowsFilterTblV2 uidTbl = uidRowsFilterParam.extractRowsFilterTbl();
        Assert.assertEquals(uidConfigs, uidTbl.getConfigs());

        RowsFilterCreateParam udlUidRowsFilterParam = getUdlThenUidRowsFilterParam();
        udlUidRowsFilterParam.checkParam();
        RowsFilterTblV2 udlUidTbl = udlUidRowsFilterParam.extractRowsFilterTbl();
        Assert.assertEquals(udlThenUidConfigs, udlUidTbl.getConfigs());

        RowsFilterCreateParam customSoaRowsFilterParam = getCustomSoaRowsFilterParam();
        customSoaRowsFilterParam.checkParam();
        RowsFilterTblV2 rowsFilterTblV2 = customSoaRowsFilterParam.extractRowsFilterTbl();
        Assert.assertEquals(customSoaConfigs, rowsFilterTblV2.getConfigs());
    }

    private RowsFilterCreateParam getCustomSoaRowsFilterParam() {
        RowsFilterCreateParam rowsFilterCreateParam = new RowsFilterCreateParam();
        rowsFilterCreateParam.setColumns(Lists.newArrayList("uid"));
        rowsFilterCreateParam.setContext("DataSyncService");
        rowsFilterCreateParam.setDrcStrategyId(32578);
        rowsFilterCreateParam.setFetchMode(FetchMode.RPC.getCode());
        rowsFilterCreateParam.setIllegalArgument(false);
        rowsFilterCreateParam.setMode(RowsFilterModeEnum.CUSTOM_SOA.getCode());
        rowsFilterCreateParam.setRouteStrategyId(0);
        rowsFilterCreateParam.setUdlColumns(null);
        return rowsFilterCreateParam;
    }


    private RowsFilterCreateParam getUdlThenUidRowsFilterParam() {
        RowsFilterCreateParam rowsFilterCreateParam = new RowsFilterCreateParam();
        rowsFilterCreateParam.setColumns(Lists.newArrayList("uid"));
        rowsFilterCreateParam.setContext("SIN");
        rowsFilterCreateParam.setDrcStrategyId(2000000002);
        rowsFilterCreateParam.setFetchMode(FetchMode.RPC.getCode());
        rowsFilterCreateParam.setIllegalArgument(false);
        rowsFilterCreateParam.setMode(RowsFilterModeEnum.TRIP_UDL_UID.getCode());
        rowsFilterCreateParam.setRouteStrategyId(0);
        rowsFilterCreateParam.setUdlColumns(Lists.newArrayList("userdata_location"));
        return rowsFilterCreateParam;
    }

    private static RowsFilterCreateParam getUdlRowsFilterParam() {
        RowsFilterCreateParam rowsFilterCreateParam = new RowsFilterCreateParam();
        rowsFilterCreateParam.setColumns(null);
        rowsFilterCreateParam.setContext("SIN");
        rowsFilterCreateParam.setDrcStrategyId(2000000002);
        rowsFilterCreateParam.setFetchMode(FetchMode.RPC.getCode());
        rowsFilterCreateParam.setIllegalArgument(false);
        rowsFilterCreateParam.setMode(RowsFilterModeEnum.TRIP_UDL.getCode());
        rowsFilterCreateParam.setRouteStrategyId(0);
        rowsFilterCreateParam.setUdlColumns(Lists.newArrayList("userdata_location"));
        return rowsFilterCreateParam;
    }

    private static RowsFilterCreateParam getUidRowsFilterParam() {
        RowsFilterCreateParam rowsFilterCreateParam = new RowsFilterCreateParam();
        rowsFilterCreateParam.setColumns(Lists.newArrayList("uid"));
        rowsFilterCreateParam.setContext("SIN");
        rowsFilterCreateParam.setDrcStrategyId(null);
        rowsFilterCreateParam.setFetchMode(FetchMode.RPC.getCode());
        rowsFilterCreateParam.setIllegalArgument(false);
        rowsFilterCreateParam.setMode(RowsFilterModeEnum.TRIP_UDL.getCode());
        rowsFilterCreateParam.setRouteStrategyId(0);
        rowsFilterCreateParam.setUdlColumns(null);
        return rowsFilterCreateParam;
    }


    @Test
    public void testToString() throws Exception {
    }

}

//Generated with love by TestMe :) Please report issues and submit feature requests at: http://weirddev.com/forum#!/testme