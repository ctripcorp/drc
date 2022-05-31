package com.ctrip.framework.drc.console.vo;

import com.ctrip.framework.drc.console.dao.entity.DataMediaTbl;
import com.ctrip.framework.drc.console.dao.entity.RowsFilterMappingTbl;
import com.ctrip.framework.drc.console.dao.entity.RowsFilterTbl;
import org.junit.Test;

import static org.junit.Assert.*;

public class RowsFilterMappingVoTest {
    

    @Test
    public void testVo() {
        RowsFilterMappingTbl mappingTbl = new RowsFilterMappingTbl();
        mappingTbl.setId(1L);
        RowsFilterTbl rowsFilterTbl = new RowsFilterTbl();
        rowsFilterTbl.setId(1L);
        rowsFilterTbl.setMode("trip_uid");
        rowsFilterTbl.setParameters("{\"columns\":[\"uid\"],\"illegalArgument\":false,\"context\":\"context.*\"}");
        DataMediaTbl dataMediaTbl = new DataMediaTbl();
        dataMediaTbl.setId(1L);
        dataMediaTbl.setNamespcae(".*");
        dataMediaTbl.setName(".*");
        dataMediaTbl.setType(0);
        dataMediaTbl.setDataMediaSourceId(1L);

        RowsFilterMappingVo vo = new RowsFilterMappingVo(mappingTbl,dataMediaTbl, rowsFilterTbl);
        System.out.println(vo.toString());
    }
    
}