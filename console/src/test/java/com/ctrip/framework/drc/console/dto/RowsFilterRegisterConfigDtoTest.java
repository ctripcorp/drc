package com.ctrip.framework.drc.console.dto;

import com.ctrip.framework.drc.console.dao.entity.DataMediaTbl;
import com.ctrip.framework.drc.console.dao.entity.RowsFilterMappingTbl;
import com.ctrip.framework.drc.console.dao.entity.RowsFilterTbl;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;
import com.ctrip.framework.drc.core.server.common.filter.row.FetchMode;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Test;

public class RowsFilterRegisterConfigDtoTest {

    @Test
    public void testDto() {
        RowsFilterConfigDto dto = new RowsFilterConfigDto();
        dto.setId(1L);
        dto.setApplierGroupId(1L);
        dto.setNamespace(".*");
        dto.setName(".*");
        dto.setType(0);
        dto.setDataMediaId(1L);
        dto.setRowsFilterId(1L);
        dto.setMode("trip_udl");
        dto.setColumns(Lists.newArrayList("columnA"));
        dto.setUdlColumns(Lists.newArrayList("columnB"));
        dto.setDrcStrategyId(0);
        dto.setRouteStrategyId(0);
        dto.setContext("context");
        dto.setIllegalArgument(false);
        dto.setFetchMode(FetchMode.RPC.getCode());
        RowsFilterMappingTbl rowsFilterMappingTbl = dto.extractRowsFilterMappingTbl();
        DataMediaTbl dataMediaTbl = dto.extractDataMediaTbl();
        RowsFilterTbl rowsFilterTbl = dto.extractRowsFilterTbl();
        RowsFilterConfig.Configs configs = JsonUtils.fromJson(rowsFilterTbl.getConfigs(), RowsFilterConfig.Configs.class);
        Assert.assertEquals(2,configs.getParameterList().size());
        System.out.println(rowsFilterTbl.getConfigs());
        System.out.println(dto.toString() + rowsFilterMappingTbl +rowsFilterTbl + dataMediaTbl );
        
    }
    
    @Test
    public void testMq() {
        String test = ".*\\..*";
        String[] split = test.split("\\\\.");
    }

}