package com.ctrip.framework.drc.console.vo;

import com.ctrip.framework.drc.console.dao.entity.DataMediaTbl;
import com.ctrip.framework.drc.console.dao.entity.RowsFilterMappingTbl;
import com.ctrip.framework.drc.console.dao.entity.RowsFilterTbl;
import com.ctrip.framework.drc.console.vo.display.RowsFilterMappingVo;
import org.junit.Assert;
import org.junit.Test;

public class RowsFilterMappingVoTest {
    

    @Test
    public void testVo() {
        RowsFilterMappingTbl mappingTbl = new RowsFilterMappingTbl();
        mappingTbl.setId(1L);
        RowsFilterTbl rowsFilterTbl = new RowsFilterTbl();
        rowsFilterTbl.setId(1L);
        rowsFilterTbl.setMode("trip_udl");
        rowsFilterTbl.setConfigs("{\n" +
                "    \"parameterList\": [\n" +
                "        {\n" +
                "            \"columns\": [\n" +
                "                \"columnB\"\n" +
                "            ],\n" +
                "            \"illegalArgument\": false,\n" +
                "            \"context\": \"context\",\n" +
                "            \"fetchMode\": 0,\n" +
                "            \"userFilterMode\": \"udl\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"columns\": [\n" +
                "                \"columnA\"\n" +
                "            ],\n" +
                "            \"illegalArgument\": false,\n" +
                "            \"context\": \"context\",\n" +
                "            \"fetchMode\": 0,\n" +
                "            \"userFilterMode\": \"uid\"\n" +
                "        }\n" +
                "    ]\n" +
                "}\n");
        DataMediaTbl dataMediaTbl = new DataMediaTbl();
        dataMediaTbl.setId(1L);
        dataMediaTbl.setNamespcae(".*");
        dataMediaTbl.setName(".*");
        dataMediaTbl.setType(0);
        dataMediaTbl.setDataMediaSourceId(1L);

        RowsFilterMappingVo vo = new RowsFilterMappingVo(mappingTbl,dataMediaTbl, rowsFilterTbl);
        Assert.assertNotNull(vo.getUdlColumns());
        Assert.assertNotNull(vo.getColumns());
        System.out.println(vo.toString());
    }
    
}