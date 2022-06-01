package com.ctrip.framework.drc.console.dto;

import com.ctrip.framework.drc.console.dao.entity.DataMediaTbl;
import com.ctrip.framework.drc.console.dao.entity.RowsFilterMappingTbl;
import com.ctrip.framework.drc.console.dao.entity.RowsFilterTbl;
import org.assertj.core.util.Lists;
import org.junit.Test;

import static org.junit.Assert.*;

public class RowsFilterConfigDtoTest {

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
        dto.setMode("trip_uid");
        dto.setColumns(Lists.newArrayList("columnA"));
        dto.setContext("context");
        dto.setIllegalArgument(false);
        RowsFilterMappingTbl rowsFilterMappingTbl = dto.getRowsFilterMappingTbl();
        DataMediaTbl dataMediaTbl = dto.getDataMediaTbl();
        RowsFilterTbl rowsFilterTbl = dto.getRowsFilterTbl();
        System.out.println(dto.toString() + rowsFilterMappingTbl +rowsFilterTbl + dataMediaTbl );
        
    }

}